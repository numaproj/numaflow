use numaflow_daemon::run_monovertex;

#[tokio::main]
async fn main() {
    run_monovertex(String::new()).await.expect("should run");
}
