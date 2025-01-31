#[allow(warnings)]
mod bindings;
use serde_json::Value as JsonValue;

use bindings::{
    exports::supabase::wrappers::routines::Guest,
    supabase::wrappers::{
        http, time,
        types::{Cell, Context, FdwError, FdwResult, OptionsType, Row, TypeOid},
        utils,
    },
};

#[derive(Debug)]
struct HubspotFdw {
    api_key: String,
    base_url: String,
    src_rows: Vec<JsonValue>,
    src_idx: usize,
    after: Option<String>,
    has_more: bool,
}

// pointer for the static FDW instance
static mut INSTANCE: *mut HubspotFdw = std::ptr::null_mut::<HubspotFdw>();

impl Default for HubspotFdw {
    fn default() -> Self {
        Self {
            api_key: String::default(),
            base_url: "https://api.hubapi.com".to_string(),
            src_rows: Vec::new(),
            src_idx: 0,
            after: None,
            has_more: false,
        }
    }
}

impl HubspotFdw {
    // initialise FDW instance
    fn init_instance() {
        let instance = Self::default();
        unsafe {
            INSTANCE = Box::leak(Box::new(instance));
        }
    }

    fn this_mut() -> &'static mut Self {
        unsafe { &mut (*INSTANCE) }
    }

    fn fetch_data(&mut self, object: &str) -> Result<(), String> {
        let endpoint = match object {
            "contacts" => "/crm/v3/objects/contacts",
            "companies" => "/crm/v3/objects/companies",
            "deals" => "/crm/v3/objects/deals",
            _ => return Err(format!("Unsupported object type: {}", object)),
        };

        let mut url = format!("{}{}", self.base_url, endpoint);
        
        // Add query parameters
        url.push_str("?limit=100");
        if let Some(after) = &self.after {
            url.push_str(&format!("&after={}", after));
        }

        let headers = vec![
            ("authorization".to_owned(), format!("Bearer {}", self.api_key)),
            ("content-type".to_owned(), "application/json".to_owned()),
        ];

        let req = http::Request {
            method: http::Method::Get,
            url,
            headers,
            body: String::default(),
        };

        let resp = http::get(&req).map_err(|e| e.to_string())?;
        let resp_json: JsonValue = serde_json::from_str(&resp.body).map_err(|e| e.to_string())?;

        // Extract pagination info
        if let Some(paging) = resp_json.get("paging") {
            self.has_more = paging.get("next").is_some();
            if self.has_more {
                self.after = paging.get("next").and_then(|v| v.get("after")).and_then(|v| v.as_str()).map(String::from);
            }
        }

        // Extract results
        if let Some(results) = resp_json.get("results") {
            if let Some(array) = results.as_array() {
                self.src_rows.extend(array.iter().cloned());
            }
        }

        Ok(())
    }
}

impl Guest for HubspotFdw {
    fn host_version_requirement() -> String {
        "^0.1.0".to_string()
    }

    fn init(ctx: &Context) -> FdwResult {
        Self::init_instance();
        let this = Self::this_mut();

        let opts = ctx.get_options(OptionsType::Server);
        this.api_key = opts.require("api_key")?;

        Ok(())
    }

    fn begin_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.src_rows.clear();
        this.src_idx = 0;
        this.after = None;
        this.has_more = false;

        let opts = ctx.get_options(OptionsType::Table);
        let object = opts.require("object")?;
        
        this.fetch_data(&object)?;
        utils::report_info(&format!("Initial fetch complete. Row count: {}", this.src_rows.len()));

        Ok(())
    }

    fn iter_scan(ctx: &Context, row: &mut Row) -> Result<Option<u32>, FdwError> {
        let this = Self::this_mut();

        if this.src_idx >= this.src_rows.len() {
            // If we have more data to fetch, get the next page
            if this.has_more {
                let opts = ctx.get_options(OptionsType::Table);
                let object = opts.require("object")?;
                this.fetch_data(&object)?;
                this.src_idx = 0;

                if this.src_idx >= this.src_rows.len() {
                    return Ok(None);
                }
            } else {
                return Ok(None);
            }
        }

        let src_row = &this.src_rows[this.src_idx];
        for tgt_col in ctx.get_columns() {
            let tgt_col_name = tgt_col.name();
            
            // Handle nested properties
            let src_value = if tgt_col_name.contains(".") {
                let parts: Vec<&str> = tgt_col_name.split('.').collect();
                let mut current = src_row;
                for part in parts {
                    current = match current.get(part) {
                        Some(v) => v,
                        None => return Err(format!("source column '{}' not found", tgt_col_name)),
                    };
                }
                current
            } else {
                match src_row.get(&tgt_col_name) {
                    Some(v) => v,
                    None => {
                        // Try to get from properties object if direct access fails
                        src_row
                            .get("properties")
                            .and_then(|props| props.get(&tgt_col_name))
                            .ok_or(format!("source column '{}' not found", tgt_col_name))?
                    }
                }
            };

            let cell = match tgt_col.type_oid() {
                TypeOid::Bool => src_value.as_bool().map(Cell::Bool),
                TypeOid::String => src_value.as_str().map(|v| Cell::String(v.to_owned())),
                TypeOid::Timestamp => {
                    src_value.as_str().and_then(|v| {
                        time::parse_from_rfc3339(v).ok().map(Cell::Timestamp)
                    })
                }
                TypeOid::Json => Some(Cell::Json(src_value.to_string())),
                _ => {
                    // Handle numeric values by converting them to strings
                    if src_value.is_number() {
                        Some(Cell::String(src_value.to_string()))
                    } else {
                        None
                    }
                }
            }.ok_or(format!(
                "cannot convert column '{}' to type {:?}",
                tgt_col_name,
                tgt_col.type_oid()
            ))?;

            row.push(cell);
        }

        this.src_idx += 1;
        Ok(Some(1))
    }

    fn re_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.src_idx = 0;
        Ok(())
    }

    fn end_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.src_rows.clear();
        this.src_idx = 0;
        this.after = None;
        this.has_more = false;
        Ok(())
    }

    fn begin_modify(_ctx: &Context) -> FdwResult {
        Err("This FDW is read-only".to_string())
    }

    fn insert(_ctx: &Context, _row: &Row) -> FdwResult {
        Err("This FDW is read-only".to_string())
    }

    fn update(_ctx: &Context, _rowid: Cell, _row: &Row) -> FdwResult {
        Err("This FDW is read-only".to_string())
    }

    fn delete(_ctx: &Context, _rowid: Cell) -> FdwResult {
        Err("This FDW is read-only".to_string())
    }

    fn end_modify(_ctx: &Context) -> FdwResult {
        Err("This FDW is read-only".to_string())
    }
}

bindings::export!(HubspotFdw with_types_in bindings);
