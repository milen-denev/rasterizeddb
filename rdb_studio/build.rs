//! Embeds the RasterizedDB icon (and version information) into the Windows
//! executable, so Explorer, the taskbar and the Start menu show the mark. Other
//! targets take the icon at run time (`src/icon.rs`) and from the `.desktop` file.

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=../branding/rasterizeddb.ico");
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        let mut res = winresource::WindowsResource::new();
        res.set_icon("../branding/rasterizeddb.ico")
            .set("FileDescription", "RasterizedDB Studio")
            .set("ProductName", "RasterizedDB Studio");
        res.compile_for("rdb_studio").expect("embedding the Windows icon in rdb_studio.exe");
    }
}
