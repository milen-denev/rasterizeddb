//! Embeds the RasterizedDB icon (and version information) into the Windows
//! server executable only, so Explorer, Task Manager and the Services console
//! show the mark. `compile_for` keeps it out of the tests and benchmarks.

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=../branding/rasterizeddb.ico");
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        let mut res = winresource::WindowsResource::new();
        res.set_icon("../branding/rasterizeddb.ico")
            .set("FileDescription", "RasterizedDB server")
            .set("ProductName", "RasterizedDB");
        res.compile_for("rasterizeddb_core").expect("embedding the Windows icon in rasterizeddb_core.exe");
    }
}
