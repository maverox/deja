use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub struct MockServer {
    pub port: u16,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl MockServer {
    pub async fn start_postgres() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, mut rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut rx => break,
                    Ok((mut socket, _)) = listener.accept() => {
                        tokio::spawn(async move {
                            let mut buf = [0u8; 1024];
                            loop {
                                let n = match socket.read(&mut buf).await {
                                    Ok(n) if n == 0 => return,
                                    Ok(n) => n,
                                    Err(_) => return,
                                };

                                // Very dumb Postgres mock
                                // If plain startup (len(4) + 196608(30000))
                                // Changed n > 8 to n >= 8 to allow exact 8-byte startup packets
                                if n >= 8 && buf[4..8] == [0, 3, 0, 0] {
                                    // Send AuthOK + ReadyForQuery
                                    // AuthOK: 'R' + len(8) + 0(OK)
                                    let auth_ok = [b'R', 0, 0, 0, 8, 0, 0, 0, 0];
                                    // ReadyForQuery: 'Z' + len(5) + 'I'
                                    let ready = [b'Z', 0, 0, 0, 5, b'I'];
                                    socket.write_all(&auth_ok).await.unwrap();
                                    socket.write_all(&ready).await.unwrap();
                                }
                                // If Query (Q)
                                else if buf[0] == b'Q' {
                                    // Send RowDescription + DataRow + CommandComplete + ReadyForQuery
                                    // RowDesc: 'T'...
                                    // DataRow: 'D'...
                                    // CmdCompl: 'C'...
                                    // Ready: 'Z'...
                                    // For simplicity, just reply with a fixed blob mimicking "SELECT 1" result

                                    // T (RowDesc) len=27 field_count=1 ... "caught_col" ...
                                    let row_desc = [
                                        b'T', 0, 0, 0, 26,
                                        0, 1, // 1 field
                                        b'c', b'o', b'l', 0, // "col"
                                        0, 0, 0, 0, // table oid
                                        0, 0, 0, 0, // type oid
                                        0, 0, 0, 23, // type: int4
                                        0, 4, // len 4
                                        255, 255, 255, 255, // type mod -1
                                        0, 0 // binary 0 (text)
                                    ];

                                    // D (DataRow) len=11 col_count=1 len=1 value='1'
                                    let data_row = [
                                        b'D', 0, 0, 0, 11,
                                        0, 1, // 1 col
                                        0, 0, 0, 1, // len 1
                                        b'1'
                                    ];

                                    // C (CommandComplete) len=13 "SELECT 1"
                                    let cmd_complete = [
                                        b'C', 0, 0, 0, 13,
                                        b'S', b'E', b'L', b'E', b'C', b'T', b' ', b'1', 0
                                    ];

                                    let ready = [b'Z', 0, 0, 0, 5, b'I'];

                                    socket.write_all(&row_desc).await.unwrap();
                                    socket.write_all(&data_row).await.unwrap();
                                    socket.write_all(&cmd_complete).await.unwrap();
                                    socket.write_all(&ready).await.unwrap();
                                }
                            }
                        });
                    }
                }
            }
        });

        Self {
            port,
            shutdown_tx: Some(tx),
        }
    }

    pub async fn start_redis() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, mut rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                     _ = &mut rx => break,
                    Ok((mut socket, _)) = listener.accept() => {
                        tokio::spawn(async move {
                            let mut buf = [0u8; 1024];
                            loop {
                                let n = match socket.read(&mut buf).await {
                                    Ok(n) if n == 0 => return,
                                    Ok(n) => n,
                                    Err(_) => return,
                                };

                                // PING check (RESP array or inline)
                                let s = std::str::from_utf8(&buf[..n]).unwrap_or("");
                                if s.to_uppercase().contains("PING") {
                                    socket.write_all(b"+PONG\r\n").await.unwrap();
                                }
                            }
                        });
                    }
                }
            }
        });

        Self {
            port,
            shutdown_tx: Some(tx),
        }
    }

    pub async fn start_generic_tcp() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, mut rx) = tokio::sync::oneshot::channel();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                     _ = &mut rx => break,
                    Ok((mut socket, _)) = listener.accept() => {
                        tokio::spawn(async move {
                            let mut buf = [0u8; 1024];
                            loop {
                                let n = match socket.read(&mut buf).await {
                                    Ok(n) if n == 0 => return,
                                    Ok(n) => n,
                                    Err(_) => return,
                                };

                                // Echo reverse
                                let mut response = buf[..n].to_vec();
                                response.reverse();
                                socket.write_all(&response).await.unwrap();
                            }
                        });
                    }
                }
            }
        });

        Self {
            port,
            shutdown_tx: Some(tx),
        }
    }

    pub fn stop(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}
