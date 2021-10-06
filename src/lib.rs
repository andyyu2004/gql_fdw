use pgx::pg_sys::{self, *};
use pgx::*;
use serde_json as json;
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr::null_mut;

pg_module_magic!();

// HACK from https://github.com/film42/hello_fdw/blob/main/src/lib.rs
#[allow(non_camel_case_types)]
type fdw_handler = pgx::PgBox<FdwRoutine>;

#[derive(Default)]
struct GraphqlFdwCtxt {
    rows: Vec<json::Value>,
    path: Vec<&'static str>,
    fields: Vec<&'static str>,
    row_index: usize,
}

#[pg_extern(raw)]
unsafe fn graphql_fdw_handler() -> fdw_handler {
    let mut ptr = PgBox::<FdwRoutine>::alloc_node(NodeTag_T_FdwRoutine);
    ptr.GetForeignRelSize = Some(get_foreign_rel_size);
    ptr.GetForeignPaths = Some(get_foreign_paths);
    ptr.GetForeignPlan = Some(get_foreign_plan);
    ptr.BeginForeignScan = Some(begin_foreign_scan);
    ptr.IterateForeignScan = Some(iterate_foreign_scan);
    ptr.ReScanForeignScan = Some(rescan_foreign_scan);
    ptr.EndForeignScan = Some(end_foreign_scan);
    ptr
}

#[pg_guard]
unsafe extern "C" fn get_foreign_rel_size(
    _root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    _foreign_table_id: Oid,
) {
    info!("get_foreign_rel_size");
    (*baserel).rows = 1.0;
}

#[pg_guard]
unsafe extern "C" fn get_foreign_paths(
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    _foreign_table_id: Oid,
) {
    info!("get_foreign_paths");
    let path = create_foreignscan_path(
        root,
        baserel,
        null_mut(),
        (*baserel).rows,
        0.0,
        0.0,
        null_mut(),
        null_mut(),
        null_mut(),
        null_mut(),
    );
    add_path(baserel, path as *mut _)
}

#[pg_guard]
unsafe extern "C" fn get_foreign_plan(
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    _foreign_table_id: Oid,
    best_path: *mut ForeignPath,
    tlist: *mut List,
    scan_clauses: *mut List,
    outer_plan: *mut Plan,
) -> *mut ForeignScan {
    info!("get_foreign_plan");

    let best_path = &mut *best_path;
    let root = &mut *root;
    let query = &mut *root.parse;
    let targets = PgList::<TargetEntry>::from_pg(query.targetList);
    // These are the output names of the column taking aliasing into account
    // Identifiers are folded to lowercase (double quote them if necessary)
    let colnames = targets.iter_ptr().map(|target| {
        let target = &mut *target;
        target.resname
    });

    let jointree = &mut *query.jointree;
    let fromlist = PgList::<Node>::from_pg(jointree.fromlist);
    assert_eq!(fromlist.iter_ptr().count(), 1, "only support single table");
    let rtr = &mut *(fromlist.iter_ptr().next().unwrap() as *mut RangeTblRef);
    let range_tbl = PgList::<RangeTblEntry>::from_pg(query.rtable);
    // Is it seriously one indexed, something seems odd?
    let entry = &mut *range_tbl.get_ptr(rtr.rtindex as usize - 1).unwrap();
    assert_eq!(entry.rtekind, RTEKind_RTE_RELATION, "only support relation rtes");
    let relname = get_rel_name(entry.relid);

    // let gql = build_gql_query_cstr(relname, colnames);
    // storing table name in first entry and the fields in the subsequent entries
    let mut private = PgList::<i8>::new();
    private.push(relname);
    for col in colnames {
        private.push(col)
    }
    best_path.fdw_private = private.into_pg();
    // just get all the field names from the query?
    // and then some quoted table name as generate some random gql?
    // e.g.
    // `select x, y, z from "a.b.c"`
    // generates
    // query {
    //   a {
    //     b {
    //       x
    //       y
    //       z
    //     }
    //   }
    // }
    make_foreignscan(
        tlist,
        extract_actual_clauses(scan_clauses, false),
        (*baserel).relid,
        null_mut(),
        best_path.fdw_private,
        null_mut(),
        null_mut(),
        outer_plan,
    )
}

fn build_gql_query_cstr<'a>(
    relname: &CStr,
    colnames: impl IntoIterator<Item = &'a CStr>,
) -> String {
    build_gql_query(relname.to_str().unwrap(), colnames.into_iter().map(|s| s.to_str().unwrap()))
}

fn build_gql_query<'a>(relname: &str, fields: impl IntoIterator<Item = &'a str>) -> String {
    let path = relname.split('.').collect::<Vec<_>>();
    let mut s = String::from("{ ");
    for component in &path {
        s.push_str(component);
        s.push_str("{");
    }
    for field in fields {
        s.push_str(" ");
        s.push_str(field);
    }

    for _ in 0..=path.len() {
        s.push_str(" }");
    }

    s
}

#[pg_guard]
unsafe extern "C" fn begin_foreign_scan(node: *mut ForeignScanState, flags: i32) {
    info!("begin foreign scan");
    if flags & EXEC_FLAG_EXPLAIN_ONLY as i32 != 0 {
        return;
    }
    let node = &mut *node;
    let foreign_plan = &mut *(node.ss.ps.plan as *mut ForeignScan);
    let private = PgList::<i8>::from_pg(foreign_plan.fdw_private);
    let relname = CStr::from_ptr(private.get_ptr(0).unwrap());
    let fields = (1..private.len()).map(|i| CStr::from_ptr(private.get_ptr(i).unwrap()));
    let gql = build_gql_query_cstr(relname, fields);
    info!("generated gql: {}", gql);

    let client = reqwest::blocking::Client::new();
    let mut map = HashMap::new();
    map.insert("query", gql);
    let response = client
        .post("http://localhost:8080/query")
        .header("Accept", "application/json")
        .header("X-MOVIO-TENANT", "dev")
        .json(&map)
        .send()
        .expect("graphql request failed");

    let text = response.text().unwrap();
    info!("response body {}", text);
    let json = json::from_str::<json::Value>(&text).unwrap();
    let mut data = &json["data"];
    let path = relname.to_str().unwrap().split('.').collect::<Vec<_>>();
    let fields = (1..private.len())
        .map(|i| CStr::from_ptr(private.get_ptr(i).unwrap()).to_str().unwrap())
        .collect::<Vec<_>>();
    for &component in &path {
        data = &data[component];
    }

    let rows = data.as_array().unwrap().clone();
    info!("rows: {:?}", rows);

    let mut ctxt = PgBox::<GraphqlFdwCtxt>::alloc0();
    ctxt.rows = rows;
    ctxt.path = path;
    ctxt.fields = fields;
    ctxt.row_index = 0;
    node.fdw_state = ctxt.into_pg() as *mut _;
}

#[pg_guard]
unsafe extern "C" fn iterate_foreign_scan(node: *mut ForeignScanState) -> *mut TupleTableSlot {
    info!("iterate_foreign_scan");
    let node = &mut *node;
    let ctxt = &mut *(node.fdw_state as *mut GraphqlFdwCtxt);
    if ctxt.row_index >= ctxt.rows.len() {
        return null_mut();
    }

    let slot = node.ss.ss_ScanTupleSlot;
    let rel = &mut *node.ss.ss_currentRelation;
    let attinmeta = TupleDescGetAttInMetadata(rel.rd_att);

    let current = &ctxt.rows[ctxt.row_index];
    let obj = match current {
        json::Value::Object(obj) => obj,
        _ => panic!("expect json object"),
    };

    let n = (*rel.rd_att).natts as usize;
    let size = std::mem::size_of::<*const c_char>() * n;
    let values = palloc0(size) as *mut *const c_char;
    let tuple_slice = std::slice::from_raw_parts_mut(values, n);

    for (i, &field) in ctxt.fields.iter().enumerate() {
        let field_value = match &obj[field] {
            json::Value::Null => continue,
            json::Value::Bool(b) => b.to_string(),
            json::Value::Number(n) => n.to_string(),
            json::Value::String(s) => s.to_owned(),
            _ => panic!("cannot select non-leaf fields"),
        };
        info!("tuple[{}] = {}", i, field_value);
        let c = CString::new(field_value).unwrap();
        let bytes = c.as_bytes_with_nul();
        let n = bytes.len();
        let ptr = palloc0(n) as *mut u8;
        let raw_slice = &mut *std::ptr::slice_from_raw_parts_mut(ptr, n);
        raw_slice.copy_from_slice(bytes);
        tuple_slice[i] = ptr as _;
    }
    let tuple = BuildTupleFromCStrings(attinmeta, values as _);
    // TODO free memory (crashes atm :))
    ExecStoreHeapTuple(tuple, slot, false);
    ctxt.row_index += 1;
    slot
}

#[pg_guard]
unsafe extern "C" fn rescan_foreign_scan(_node: *mut ForeignScanState) {
    todo!("rescan foreign scan")
}

#[pg_guard]
unsafe extern "C" fn end_foreign_scan(_node: *mut ForeignScanState) {
    info!("ending");
    // panic!();
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use super::*;

    #[pg_test]
    fn test_query_filter_processes() {
        Spi::run("CREATE FOREIGN DATA WRAPPER graphql_fdw HANDLER graphql_fdw_handler");
        Spi::run("CREATE SERVER graphql_server FOREIGN DATA WRAPPER graphql_fdw");
        Spi::run(
            "CREATE FOREIGN TABLE \"team.core.service.filters.processes\" (name text) SERVER graphql_server",
        );
        let name =
            Spi::get_one::<String>("SELECT name FROM \"team.core.service.filters.processes\"");
        dbg!(name);
    }

    // #[pg_test]
    // fn test_select() {
    //     Spi::run("CREATE FOREIGN DATA WRAPPER graphql_fdw HANDLER graphql_fdw_handler");
    //     Spi::run("CREATE SERVER graphql_server FOREIGN DATA WRAPPER graphql_fdw");
    //     // Spi::run("CREATE FOREIGN TABLE \"a.b.c\" (x text, y text, z text) SERVER graphql_server");
    //     // Spi::get_two::<String, String>("SELECT x, y, z FROM \"a.b.c\"");
    //     Spi::run(
    //         "CREATE FOREIGN TABLE test_ft (w int, x text, y text, z text) SERVER graphql_server",
    //     );
    //     Spi::get_two::<String, String>("SELECT w as k, x, y, z FROM test_ft");
    //     Spi::run(
    //         "CREATE FOREIGN TABLE \"a.b.c\" (w int, x text, y text, z text) SERVER graphql_server",
    //     );
    //     Spi::get_two::<String, String>("SELECT w as k, x, y, z FROM \"a.b.c\"");
    // }

    #[test]
    fn test_build_gql() {
        println!("{}", build_gql_query("x.y.z", ["x", "y", "z"]));
    }
}

#[cfg(test)]
pub mod pg_test {
    use super::*;
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
        // Spi::run("CREATE FOREIGN DATA WRAPPER graphql_fdw HANDLER graphql_fdw_handler");
        // Spi::run("CREATE SERVER graphql_server FOREIGN DATA WRAPPER graphql_fdw");
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
