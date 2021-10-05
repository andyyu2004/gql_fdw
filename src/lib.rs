use std::ptr::null_mut;

use pgx::pg_sys::*;
use pgx::*;

pg_module_magic!();

#[pg_extern(raw)]
unsafe fn graphql_fdw_handler() -> Datum {
    let ptr = palloc(std::mem::size_of::<FdwRoutine>()) as *mut FdwRoutine;
    *ptr = FdwRoutine {
        GetForeignRelSize: Some(get_foreign_rel_size),
        GetForeignPaths: Some(get_foreign_paths),
        GetForeignPlan: Some(get_foreign_plan),
        BeginForeignScan: Some(begin_foreign_scan),
        IterateForeignScan: Some(iterate_foreign_scan),
        ReScanForeignScan: Some(rescan_foreign_scan),
        EndForeignScan: Some(end_foreign_scan),
        ..Default::default()
    };
    ptr as usize
}

unsafe extern "C" fn get_foreign_rel_size(
    _root: *mut PlannerInfo,
    _baserel: *mut RelOptInfo,
    _foreign_table_id: Oid,
) {
}

unsafe extern "C" fn get_foreign_paths(
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    _foreign_table_id: Oid,
) {
    let path = create_foreignscan_path(
        root,
        baserel,
        null_mut(),
        0.0,
        0.0,
        0.0,
        null_mut(),
        null_mut(),
        null_mut(),
        null_mut(),
    );
    add_path(baserel, path as *mut _)
}

unsafe extern "C" fn begin_foreign_scan(node: *mut ForeignScanState, flags: i32) {
    todo!()
}

unsafe extern "C" fn rescan_foreign_scan(node: *mut ForeignScanState) {
    todo!()
}

unsafe extern "C" fn end_foreign_scan(node: *mut ForeignScanState) {
    todo!()
}

unsafe extern "C" fn iterate_foreign_scan(node: *mut ForeignScanState) -> *mut TupleTableSlot {
    (*node).fdw_state;
    todo!()
}

unsafe extern "C" fn get_foreign_plan(
    root: *mut PlannerInfo,
    baserel: *mut RelOptInfo,
    foreign_table_id: Oid,
    best_path: *mut ForeignPath,
    tlist: *mut List,
    scan_clauses: *mut List,
    plan: *mut Plan,
) -> *mut ForeignScan {
    todo!()
}

#[pg_extern]
fn hello_graphql_fdw() -> &'static str {
    "Hello, graphql_fdw"
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use pgx::*;

    #[pg_test]
    fn test_hello_graphql_fdw() {
        assert_eq!("Hello, graphql_fdw", crate::hello_graphql_fdw());
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
