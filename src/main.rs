use std::io::Write;
use std::ops::Add;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::sleep;
use std::time::Duration;
use redis::{Client, cmd, pipe, RedisResult};

fn main() {
    let client = Arc::new(Mutex::new(match establish_connection() {
        Ok(cl) => cl,
        Err(err) => {
            eprintln!("Could not connect to database: {err}");
            return;
        }
    }));

    clear_db(Arc::clone(&client)).unwrap();

    let (before_inc, after_incr) = create_and_increment(Arc::clone(&client), 61).unwrap();
    println!("Created variable with value = {before_inc}. After incrementing = {after_incr}");

    execute(client);
}

fn create_and_increment(client: Arc<Mutex<Client>>, initial: i32) -> RedisResult<(i32, i32)> {
    let client_lock = client.lock().unwrap();
    let mut con = client_lock.get_connection()?;

    let results: (i32, i32) = pipe().atomic()
        .cmd("SET").arg("counter").arg(initial).ignore()
        .cmd("GET").arg("counter")
        .cmd("INCR").arg("counter").ignore()
        .cmd("GET").arg("counter").query(&mut con)?;

    Ok(results)
}

fn execute(client: Arc<Mutex<Client>>) {
    let list_name = "queue";

    let producer = thread::spawn({
        let client = Arc::clone(&client);

        move || {
            (0..50).for_each({
                |i| {
                    let mut con = client.lock().unwrap().get_connection().unwrap();

                    let msg = format!("message_{i}");
                    let _: () = cmd("RPUSH").arg(list_name).arg(msg.clone()).query(&mut con).unwrap();

                    println!("PRODUCER:: sent '{msg}'");
                    sleep(Duration::from_millis(150));
                }
            });
        }
    });

    let consumer = thread::spawn({
        let client = Arc::clone(&client);

        move || {
            let timeout_sec = 60;
            let mut output_file = std::fs::File::create("test/output.txt").unwrap();

            (0..50).for_each(|_| {
                let mut con = client.lock().unwrap().get_connection().unwrap();

                let (_, msg): (String, String) = cmd("BLPOP").arg(list_name).arg(timeout_sec).query(&mut con).unwrap();

                output_file.write(msg.clone().add("\n").as_bytes()).unwrap();
                println!("CONSUMER:: received '{msg}'");
                sleep(Duration::from_millis(200));
            });
        }
    });

    producer.join().unwrap();
    consumer.join().unwrap();
}

fn establish_connection() -> RedisResult<Client> {
    Ok(Client::open("redis://127.0.0.1:6379")?)
}

fn clear_db(client: Arc<Mutex<Client>>) -> RedisResult<()> {
    let client_lock = client.lock().unwrap();
    let mut con = client_lock.get_connection()?;

    let _: () = cmd("FLUSHDB").query(&mut con).unwrap();

    Ok(())
}