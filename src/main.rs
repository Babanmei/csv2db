use std::{io};
use csv2db::{cli, App};


fn main() {
    let mut app = App::from_args(cli()).expect("链接数据库失败");

    app.import_from_stdin(io::stdin());


}

