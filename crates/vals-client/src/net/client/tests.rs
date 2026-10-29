    use super::*;

    mod reverse_scan;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::os::fd::AsRawFd;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{mpsc, Arc};
    use std::task::Wake;

    fn read_frame(stream: &mut impl Read) -> Vec<u8> {
        let mut len = [0u8; 4];
        stream.read_exact(&mut len).unwrap();
        let mut frame = vec![0u8; u32::from_le_bytes(len) as usize];
        stream.read_exact(&mut frame).unwrap();
        frame
    }

    fn write_frame(stream: &mut impl Write, frame: &[u8]) {
        stream.write_all(frame).unwrap();
    }

    fn encoded_response_server(
        response: Response,
    ) -> (SocketAddr, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let _hello = read_frame(&mut stream);
            stream.write_all(&crate::proto::encode_hello()).unwrap();
            let request = read_frame(&mut stream);
            let id = u64::from_le_bytes(request[..8].try_into().unwrap());
            let payload = wire::encode_response_payload(&response);
            write_frame(&mut stream, &frame_msg(id, &payload).unwrap());
        });
        (addr, server)
    }

    #[test]
    fn multi_scan_cold_keys_only_connection_never_requests_values() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            read_frame(&mut stream);
            stream.write_all(&crate::proto::encode_hello()).unwrap();
            for i in 0..2 {
                let frame = read_frame(&mut stream);
                let id = u64::from_le_bytes(frame[..8].try_into().unwrap());
                let request = wire::decode_request(&frame[8..]).unwrap();
                let response = if i == 0 {
                    assert_eq!(request, wire::OwnedRequest::GetWatermark);
                    Response::Watermark(0)
                } else {
                    assert!(matches!(request, wire::OwnedRequest::MultiScan { keys_only: true, .. }), "{request:?}");
                    Response::MultiScan { start: 0, more: false, keys_only: true, pages: vec![ScanPage {
                        entries: vec![(b"a".to_vec(), Stored { version: crate::hlc::Version { hlc: 9, node: 1 }, value: Some(Vec::new()) })],
                        ..Default::default()
                    }] }
                };
                write_frame(&mut stream, &frame_msg(id, &wire::encode_response_payload(&response)).unwrap());
            }
        });
        let transport = NetTransport::connect(&[(1, addr)]);
        let out = vals_rt::block_on(transport.scan_many(1, 2, &[(None, None, 1)], true), reactor::poll_io);
        assert_eq!(out[0].as_ref().unwrap().entries[0].1.value, Some(Vec::new()));
        server.join().unwrap();
    }

    #[test]
    fn multi_scan_falls_back_only_for_peers_without_the_feature() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            read_frame(&mut stream);
            let mut hello = crate::proto::encode_hello();
            let n = hello.len();
            hello[n-8..].copy_from_slice(&(crate::proto::FEATURES & !crate::proto::FEATURE_MULTI_SCAN).to_le_bytes());
            stream.write_all(&hello).unwrap();
            let probe = read_frame(&mut stream);
            assert_eq!(wire::decode_request(&probe[8..]).unwrap(), wire::OwnedRequest::GetWatermark);
            let id = u64::from_le_bytes(probe[..8].try_into().unwrap());
            write_frame(&mut stream, &frame_msg(id, &wire::encode_response_payload(&Response::Watermark(0))).unwrap());
            for _ in 0..6 {
                let frame = read_frame(&mut stream);
                let id = u64::from_le_bytes(frame[..8].try_into().unwrap());
                let wire::OwnedRequest::Scan { lo, .. } = wire::decode_request(&frame[8..]).unwrap() else { panic!("old peer got a new opcode"); };
                let response = Response::Scan {
                    entries: vec![(lo.unwrap(), Stored { version: crate::hlc::Version { hlc: 9, node: 1 }, value: Some(b"value".to_vec()) })],
                    more: false, fragments: vec![], resume: None,
                };
                write_frame(&mut stream, &frame_msg(id, &wire::encode_response_payload(&response)).unwrap());
            }
        });
        let transport = NetTransport::connect(&[(1, addr)]);
        let scans = [(Some(&b"a"[..]), Some(&b"b"[..]), 1), (Some(&b"c"[..]), None, 1), (Some(&b"d"[..]), None, 2)];
        for keys_only in [false, true] {
            let out = vals_rt::block_on(transport.scan_many(1, 2, &scans, keys_only), reactor::poll_io);
            for (i, page) in out.into_iter().enumerate() {
                let page = page.unwrap();
                assert_eq!(page.entries[0].0, scans[i].0.unwrap());
                assert_eq!(page.entries[0].1.version.hlc, 9);
                assert_eq!(page.entries[0].1.value.as_deref(), Some(if keys_only { &b""[..] } else { &b"value"[..] }));
            }
        }
        server.join().unwrap();
    }

    fn reverse_reply_server(
        listener: TcpListener,
        ready: Option<mpsc::Sender<()>>,
        release: Option<mpsc::Receiver<()>>,
        only_second: bool,
    ) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            assert_eq!(
                crate::proto::decode_hello(&read_frame(&mut stream)),
                Ok((crate::proto::PROTO_VERSION, crate::proto::FEATURES))
            );
            stream.write_all(&crate::proto::encode_hello()).unwrap();
            let first = read_frame(&mut stream);
            let second = read_frame(&mut stream);
            let first_id = u64::from_le_bytes(first[..8].try_into().unwrap());
            let second_id = u64::from_le_bytes(second[..8].try_into().unwrap());
            if let Some(ready) = ready {
                ready.send(()).unwrap();
            }
            if let Some(release) = release {
                release.recv().unwrap();
            }
            write_frame(&mut stream, &frame_msg(second_id, b"second-reply").unwrap());
            if !only_second {
                write_frame(&mut stream, &frame_msg(first_id, b"first-reply").unwrap());
                std::thread::sleep(Duration::from_millis(100));
            } else {
                std::thread::sleep(Duration::from_millis(100));
            }
        })
    }

    #[test]
    fn multiplexed_socket_replies_resolve_by_request_id_out_of_order() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = reverse_reply_server(listener, None, None, false);
        let conn = Rc::new(MuxConn::new(addr));
        let replies = vals_rt::block_on(
            vals_rt::join_all(vec![
                MuxRpc::new(conn.clone(), b"first-request".to_vec()),
                MuxRpc::new(conn, b"second-request".to_vec()),
            ]),
            || {
                flush_dirty();
                reactor::poll_io();
            },
        );

        assert!(matches!(&replies[0], Ok(reply) if reply == b"first-reply"));
        assert!(matches!(&replies[1], Ok(reply) if reply == b"second-reply"));
        server.join().unwrap();
    }

    #[test]
    fn expired_driver_still_drains_readable_sibling_reply() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let server = reverse_reply_server(
            listener,
            Some(ready_tx),
            Some(release_rx),
            true,
        );
        let conn = Rc::new(MuxConn::new(addr));
        conn.st.borrow_mut().rpc_deadline = Some(Duration::from_secs(10));
        register_deadline_conn(&conn);
        let mut first = Box::pin(MuxRpc::new(conn.clone(), b"first-request".to_vec()));
        let mut second = Box::pin(MuxRpc::new(conn.clone(), b"second-request".to_vec()));
        let waker = Waker::noop().clone();
        let mut cx = Context::from_waker(&waker);

        assert!(first.as_mut().poll(&mut cx).is_pending());
        assert!(second.as_mut().poll(&mut cx).is_pending());
        loop {
            assert!(first.as_mut().poll(&mut cx).is_pending());
            assert!(second.as_mut().poll(&mut cx).is_pending());
            flush_dirty();
            reactor::poll_io();
            if ready_rx.try_recv().is_ok() {
                break;
            }
        }
        {
            let mut state = conn.st.borrow_mut();
            state.driver = Some(first.id);
            state.pending.get_mut(&first.id).unwrap().deadline_at =
                Some(Instant::now() - Duration::from_millis(1));
            state.next_deadline = Some(Instant::now() - Duration::from_millis(1));
        }
        drive_deadlines();
        assert!(matches!(
            conn.st.borrow().pending[&first.id].result,
            Some(Err(TransportError::Timeout))
        ));
        release_tx.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(10));

        assert!(matches!(
            first.as_mut().poll(&mut cx),
            Poll::Ready(Err(TransportError::Timeout))
        ));
        assert!(matches!(
            second.as_mut().poll(&mut cx),
            Poll::Ready(Ok(reply)) if reply == b"second-reply"
        ));
        server.join().unwrap();
    }

    struct CountWaker(AtomicUsize);

    impl Wake for CountWaker {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn delayed_hello_parks_on_read_then_releases_queued_request() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let (hello_read_tx, hello_read_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let _hello = read_frame(&mut stream);
            hello_read_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            stream.write_all(&crate::proto::encode_hello()).unwrap();
            let request = read_frame(&mut stream);
            let id = u64::from_le_bytes(request[..8].try_into().unwrap());
            write_frame(&mut stream, &frame_msg(id, b"reply").unwrap());
        });

        let conn = Rc::new(MuxConn::new(addr));
        let mut rpc = Box::pin(MuxRpc::new(conn, b"request".to_vec()));
        let wake_count = Arc::new(CountWaker(AtomicUsize::new(0)));
        let waker = Waker::from(wake_count.clone());
        let mut cx = Context::from_waker(&waker);

        assert!(rpc.as_mut().poll(&mut cx).is_pending());
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            flush_dirty();
            if hello_read_rx.recv_timeout(Duration::from_millis(10)).is_ok() {
                break;
            }
            assert!(Instant::now() < deadline, "the hello never reached the server");
        }
        assert!(rpc.as_mut().poll(&mut cx).is_pending());
        wake_count.0.store(0, Ordering::Relaxed);
        for _ in 0..8 {
            reactor::poll_io_nonblocking();
        }
        let writable_wakes = wake_count.0.load(Ordering::Relaxed);

        release_tx.send(()).unwrap();
        assert_eq!(writable_wakes, 0, "delayed hello must not wake on writable");
        let deadline = Instant::now() + Duration::from_secs(2);
        let reply = loop {
            reactor::poll_io();
            if wake_count.0.swap(0, Ordering::Relaxed) != 0 {
                match rpc.as_mut().poll(&mut cx) {
                    Poll::Ready(result) => break result,
                    Poll::Pending => flush_dirty(),
                }
            }
            assert!(Instant::now() < deadline, "request did not resume after hello");
        };
        assert!(matches!(reply, Ok(bytes) if bytes == b"reply"));
        server.join().unwrap();
    }

    #[test]
    fn reply_then_close_in_one_drain_delivers_the_reply() {
        let (client, mut server) = std::os::unix::net::UnixStream::pair().unwrap();
        client.set_nonblocking(true).unwrap();
        let conn = Rc::new(MuxConn::new("127.0.0.1:1".parse().unwrap()));
        {
            let mut state = conn.st.borrow_mut();
            state.fd = client.as_raw_fd();
            state.sock = Some(super::super::Sock::Unix(client));
            state.set_negotiated_for_test();
        }
        let mut answered = Box::pin(MuxRpc::new(conn.clone(), b"answered".to_vec()));
        let mut unanswered = Box::pin(MuxRpc::new(conn.clone(), b"unanswered".to_vec()));
        let waker = Waker::noop().clone();
        let mut cx = Context::from_waker(&waker);
        assert!(answered.as_mut().poll(&mut cx).is_pending());
        assert!(unanswered.as_mut().poll(&mut cx).is_pending());
        flush_dirty();
        let first = read_frame(&mut server);
        let second = read_frame(&mut server);
        assert_eq!(&first[8..], b"answered");
        assert_eq!(&second[8..], b"unanswered");
        let id = u64::from_le_bytes(first[..8].try_into().unwrap());
        write_frame(&mut server, &frame_msg(id, b"reply").unwrap());
        drop(server);

        conn.st.borrow_mut().drain_reads(&mut Vec::new());
        assert!(conn.st.borrow().sock.is_none());
        assert!(matches!(
            answered.as_mut().poll(&mut cx),
            Poll::Ready(Ok(reply)) if reply == b"reply"
        ));
        assert!(matches!(
            unanswered.as_mut().poll(&mut cx),
            Poll::Ready(Err(TransportError::Unavailable))
        ));
        assert!(conn.st.borrow().finished.is_empty());
    }

    #[test]
    fn cancelled_unsent_rpc_discards_storage_before_releasing_credit() {
        let (client, _server) = std::os::unix::net::UnixStream::pair().unwrap();
        client.set_nonblocking(true).unwrap();
        let pool = Rc::new(RefCell::new(CreditPool::from_budgets(
            crate::coord::ClientBudgets {
                max_ops: 2,
                max_bytes: 4096,
                node_max_ops: 1,
                node_max_bytes: 4096,
            },
        )));
        let conn = Rc::new(MuxConn::new_accounted(
            "127.0.0.1:1".parse().unwrap(),
            7,
            pool.clone(),
        ));
        {
            let mut state = conn.st.borrow_mut();
            state.fd = client.as_raw_fd();
            state.sock = Some(super::super::Sock::Unix(client));
            state.set_negotiated_for_test();
        }
        let credit = try_transport_credit(&pool, 7, 1, 128).unwrap();
        let (op_credit, request_credit) = credit.split_ops_bytes();
        let mut rpc = Box::pin(MuxRpc::new_accounted(
            conn.clone(),
            b"cancelled".to_vec(),
            op_credit,
            request_credit,
            false,
        ));
        let waker = Waker::noop().clone();
        let mut cx = Context::from_waker(&waker);
        assert!(rpc.as_mut().poll(&mut cx).is_pending());
        assert_eq!(pool.borrow().stats().in_flight_ops, 1);

        drop(rpc);
        assert_eq!(pool.borrow().stats().in_flight_ops, 0);
        assert_eq!(pool.borrow().stats().in_flight_bytes, 0);
        let state = conn.st.borrow();
        assert_eq!(state.pending.len(), 0);
        assert!(state.sock.is_some());
        assert!(state.out.is_empty());
        assert_eq!(state.out.capacity(), 0);
    }

    #[test]
    fn timed_out_unsent_rpc_discards_storage_before_releasing_credit() {
        let (client, _server) = std::os::unix::net::UnixStream::pair().unwrap();
        client.set_nonblocking(true).unwrap();
        let pool = Rc::new(RefCell::new(CreditPool::from_budgets(
            crate::coord::ClientBudgets {
                max_ops: 1,
                max_bytes: 4096,
                node_max_ops: 1,
                node_max_bytes: 4096,
            },
        )));
        let conn = Rc::new(MuxConn::new_accounted(
            "127.0.0.1:1".parse().unwrap(),
            7,
            pool.clone(),
        ));
        {
            let mut state = conn.st.borrow_mut();
            state.fd = client.as_raw_fd();
            state.sock = Some(super::super::Sock::Unix(client));
            state.set_negotiated_for_test();
        }
        let credit = try_transport_credit(&pool, 7, 1, 128).unwrap();
        let (op_credit, request_credit) = credit.split_ops_bytes();
        let mut rpc = Box::pin(MuxRpc::new_accounted(
            conn.clone(),
            b"timeout".to_vec(),
            op_credit,
            request_credit,
            false,
        ));
        let waker = Waker::noop().clone();
        let mut cx = Context::from_waker(&waker);
        assert!(rpc.as_mut().poll(&mut cx).is_pending());
        {
            let mut state = conn.st.borrow_mut();
            state.pending.get_mut(&rpc.id).unwrap().result =
                Some(Err(TransportError::Timeout));
        }
        assert!(matches!(
            rpc.as_mut().poll(&mut cx),
            Poll::Ready(Err(TransportError::Timeout))
        ));
        drop(rpc);
        assert_eq!(pool.borrow().stats().in_flight_ops, 0);
        assert_eq!(pool.borrow().stats().in_flight_bytes, 0);
        let state = conn.st.borrow();
        assert_eq!(state.pending.len(), 0);
        assert!(state.sock.is_some());
        assert!(state.out.is_empty());
        assert_eq!(state.out.capacity(), 0);
    }

    fn accounted_rpc(
        conn: Rc<MuxConn>,
        pool: &Rc<RefCell<CreditPool>>,
        node: NodeId,
        payload: &[u8],
    ) -> MuxRpc {
        let credit = try_transport_credit(pool, node, 1, payload.len() + 12).unwrap();
        let (op_credit, request_credit) = credit.split_ops_bytes();
        MuxRpc::new_accounted(
            conn,
            payload.to_vec(),
            op_credit,
            request_credit,
            false,
        )
    }

    #[test]
    fn bounded_cancelled_unsent_request_splices_and_sibling_succeeds() {
        let (client, mut server) = std::os::unix::net::UnixStream::pair().unwrap();
        client.set_nonblocking(true).unwrap();
        let pool = Rc::new(RefCell::new(CreditPool::from_budgets(
            crate::coord::ClientBudgets {
                max_ops: 2,
                max_bytes: 4096,
                node_max_ops: 2,
                node_max_bytes: 4096,
            },
        )));
        let conn = Rc::new(MuxConn::new_accounted(
            "127.0.0.1:1".parse().unwrap(),
            7,
            pool.clone(),
        ));
        {
            let mut state = conn.st.borrow_mut();
            state.fd = client.as_raw_fd();
            state.sock = Some(super::super::Sock::Unix(client));
            state.set_negotiated_for_test();
        }
        let server = std::thread::spawn(move || {
            let request = read_frame(&mut server);
            assert_eq!(&request[8..], b"survivor");
            let id = u64::from_le_bytes(request[..8].try_into().unwrap());
            write_frame(&mut server, &frame_msg(id, b"reply").unwrap());
            std::thread::sleep(Duration::from_millis(100));
        });
        let mut cancelled = Box::pin(accounted_rpc(conn.clone(), &pool, 7, b"cancelled"));
        let mut survivor = Box::pin(accounted_rpc(conn.clone(), &pool, 7, b"survivor"));
        let waker = Waker::noop().clone();
        let mut cx = Context::from_waker(&waker);
        assert!(cancelled.as_mut().poll(&mut cx).is_pending());
        assert!(survivor.as_mut().poll(&mut cx).is_pending());

        drop(cancelled);
        {
            let state = conn.st.borrow();
            assert!(state.sock.is_some());
            assert_eq!(state.pending.len(), 1);
            assert_eq!(state.out.len(), b"survivor".len() + 12);
            assert_eq!(state.out.capacity(), state.out.len());
            assert_eq!(state.enqueued_total, state.out.len() as u64);
        }
        let stats = pool.borrow().stats();
        assert_eq!(stats.in_flight_ops, 1);
        assert_eq!(stats.in_flight_bytes, b"survivor".len() + 12);

        let reply = vals_rt::block_on(survivor.as_mut(), || {
            flush_dirty();
            reactor::poll_io();
        });
        assert!(matches!(&reply, Ok(bytes) if bytes == b"reply"), "{reply:?}");
        assert_eq!(pool.borrow().stats().in_flight_ops, 0);
        server.join().unwrap();
    }

    #[test]
    fn unbounded_cancelled_pre_handshake_request_splices_and_sibling_succeeds() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let _hello = read_frame(&mut stream);
            stream.write_all(&crate::proto::encode_hello()).unwrap();
            let request = read_frame(&mut stream);
            assert_eq!(&request[8..], b"survivor");
            let id = u64::from_le_bytes(request[..8].try_into().unwrap());
            write_frame(&mut stream, &frame_msg(id, b"reply").unwrap());
            std::thread::sleep(Duration::from_millis(100));
        });
        let conn = Rc::new(MuxConn::new(addr));
        let mut cancelled = Box::pin(MuxRpc::new(conn.clone(), b"cancelled".to_vec()));
        let mut survivor = Box::pin(MuxRpc::new(conn.clone(), b"survivor".to_vec()));
        let waker = Waker::noop().clone();
        let mut cx = Context::from_waker(&waker);
        assert!(cancelled.as_mut().poll(&mut cx).is_pending());
        assert!(survivor.as_mut().poll(&mut cx).is_pending());

        drop(cancelled);
        {
            let state = conn.st.borrow();
            assert!(state.sock.is_some());
            assert!(state.negotiated.is_none());
            assert_eq!(state.pending.len(), 1);
            assert_eq!(
                state.out.len(),
                4 + crate::proto::HELLO_BODY_LEN + b"survivor".len() + 12
            );
            assert_eq!(state.out.capacity(), state.out.len());
        }

        let reply = vals_rt::block_on(survivor.as_mut(), || {
            flush_dirty();
            reactor::poll_io();
        });
        assert!(matches!(reply, Ok(bytes) if bytes == b"reply"));
        server.join().unwrap();
    }

    #[test]
    fn timed_out_wholly_unsent_request_splices_and_sibling_succeeds() {
        let (client, mut server) = std::os::unix::net::UnixStream::pair().unwrap();
        client.set_nonblocking(true).unwrap();
        let conn = Rc::new(MuxConn::new("127.0.0.1:1".parse().unwrap()));
        {
            let mut state = conn.st.borrow_mut();
            state.fd = client.as_raw_fd();
            state.sock = Some(super::super::Sock::Unix(client));
            state.set_negotiated_for_test();
        }
        let server = std::thread::spawn(move || {
            let request = read_frame(&mut server);
            assert_eq!(&request[8..], b"survivor");
            let id = u64::from_le_bytes(request[..8].try_into().unwrap());
            write_frame(&mut server, &frame_msg(id, b"reply").unwrap());
            std::thread::sleep(Duration::from_millis(100));
        });
        let mut expired = Box::pin(MuxRpc::new(conn.clone(), b"expired".to_vec()));
        let mut survivor = Box::pin(MuxRpc::new(conn.clone(), b"survivor".to_vec()));
        let waker = Waker::noop().clone();
        let mut cx = Context::from_waker(&waker);
        assert!(expired.as_mut().poll(&mut cx).is_pending());
        assert!(survivor.as_mut().poll(&mut cx).is_pending());
        conn.st.borrow_mut().pending.get_mut(&expired.id).unwrap().result =
            Some(Err(TransportError::Timeout));

        assert!(matches!(
            expired.as_mut().poll(&mut cx),
            Poll::Ready(Err(TransportError::Timeout))
        ));
        assert_eq!(conn.st.borrow().pending.len(), 1);
        let reply = vals_rt::block_on(survivor.as_mut(), || {
            flush_dirty();
            reactor::poll_io();
        });
        assert!(matches!(reply, Ok(bytes) if bytes == b"reply"));
        server.join().unwrap();
    }

    #[test]
    fn partially_sent_cancellation_tears_down_with_neutral_collateral_error() {
        let (client, _server) = std::os::unix::net::UnixStream::pair().unwrap();
        client.set_nonblocking(true).unwrap();
        let pool = Rc::new(RefCell::new(CreditPool::from_budgets(
            crate::coord::ClientBudgets {
                max_ops: 2,
                max_bytes: 4096,
                node_max_ops: 2,
                node_max_bytes: 4096,
            },
        )));
        let conn = Rc::new(MuxConn::new_accounted(
            "127.0.0.1:1".parse().unwrap(),
            7,
            pool.clone(),
        ));
        {
            let mut state = conn.st.borrow_mut();
            state.fd = client.as_raw_fd();
            state.sock = Some(super::super::Sock::Unix(client));
            state.set_negotiated_for_test();
        }
        let mut partial = Box::pin(accounted_rpc(conn.clone(), &pool, 7, b"partial"));
        let mut collateral = Box::pin(accounted_rpc(conn.clone(), &pool, 7, b"collateral"));
        let waker = Waker::noop().clone();
        let mut cx = Context::from_waker(&waker);
        assert!(partial.as_mut().poll(&mut cx).is_pending());
        assert!(collateral.as_mut().poll(&mut cx).is_pending());
        {
            let mut state = conn.st.borrow_mut();
            state.sent_total = 1;
            state.out_pos = 1;
        }

        drop(partial);
        {
            let state = conn.st.borrow();
            assert!(state.sock.is_none());
            assert!(state.out.is_empty());
            assert_eq!(state.out.capacity(), 0);
            assert!(state.pending.is_empty());
        }
        assert_eq!(pool.borrow().stats().in_flight_ops, 0);
        assert_eq!(pool.borrow().stats().in_flight_bytes, 0);
        assert!(matches!(
            collateral.as_mut().poll(&mut cx),
            Poll::Ready(Err(TransportError::Overloaded))
        ));
    }

    #[test]
    fn node_blocked_transport_waiter_does_not_gate_disjoint_node() {
        let pool = Rc::new(RefCell::new(CreditPool::from_budgets(
            crate::coord::ClientBudgets {
                max_ops: 2,
                max_bytes: 4096,
                node_max_ops: 1,
                node_max_bytes: 4096,
            },
        )));
        let held = try_transport_credit(&pool, 1, 1, 32).unwrap();
        let mut blocked = Box::pin(Acquire {
            pool: pool.clone(),
            ops: 1,
            bytes: 32,
            node_costs: vec![NodeCreditCost {
                node: 1,
                ops: 1,
                bytes: 32,
            }],
            ticket: None,
            deadline_at: None,
        });
        let waker = Waker::noop().clone();
        let mut cx = Context::from_waker(&waker);
        assert!(blocked.as_mut().poll(&mut cx).is_pending());
        let healthy = vals_rt::block_on(
            Acquire {
                pool: pool.clone(),
                ops: 1,
                bytes: 32,
                node_costs: vec![NodeCreditCost {
                    node: 2,
                    ops: 1,
                    bytes: 32,
                }],
                ticket: None,
                deadline_at: None,
            },
            || {},
        )
        .unwrap();
        assert_eq!(pool.borrow().stats().in_flight_ops, 2);
        drop(healthy);
        drop(held);
        assert!(matches!(blocked.as_mut().poll(&mut cx), Poll::Ready(Ok(_))));
    }

    #[test]
    fn connection_failure_releases_all_accounted_pending_work() {
        let pool = Rc::new(RefCell::new(CreditPool::from_budgets(
            crate::coord::ClientBudgets {
                max_ops: 2,
                max_bytes: 4096,
                node_max_ops: 2,
                node_max_bytes: 4096,
            },
        )));
        let conn = Rc::new(MuxConn::new_accounted(
            "127.0.0.1:1".parse().unwrap(),
            9,
            pool.clone(),
        ));
        let first = try_transport_credit(&pool, 9, 1, 32).unwrap();
        let second = try_transport_credit(&pool, 9, 1, 32).unwrap();
        {
            let mut state = conn.st.borrow_mut();
            state.pending.insert(
                1,
                Waiter {
                    op_credit: Some(first),
                    ..Default::default()
                },
            );
            state.pending.insert(
                2,
                Waiter {
                    op_credit: Some(second),
                    ..Default::default()
                },
            );
            state.reset_failed(&mut Vec::new());
        }
        assert_eq!(pool.borrow().stats().in_flight_ops, 0);
        assert_eq!(pool.borrow().stats().in_flight_bytes, 0);
    }

    #[test]
    fn connection_generation_counter_survives_resets() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let mut state = ConnState::default();
        state.ensure_conn(address).unwrap();
        assert_eq!(state.connection_generations, 1);
        state.reset_failed(&mut Vec::new());
        state.ensure_conn(address).unwrap();
        assert_eq!(state.connection_generations, 2);
    }

    #[test]
    fn reset_discards_unaccounted_partial_frame_bytes_on_every_reconnect() {
        let mut state = ConnState::default();
        for _ in 0..32 {
            let (client, mut server) = std::os::unix::net::UnixStream::pair().unwrap();
            client.set_nonblocking(true).unwrap();
            state.fd = client.as_raw_fd();
            state.sock = Some(super::super::Sock::Unix(client));
            state.set_negotiated_for_test();
            server.write_all(&(1024u32 * 1024).to_le_bytes()).unwrap();
            server.write_all(&[7u8; 1024]).unwrap();
            state.drain_reads(&mut Vec::new());
            assert!(state.reader.buffered_len() >= 1028);
            assert_eq!(state.reader_bytes, 0);
            state.reset_failed(&mut Vec::new());
            assert_eq!(state.reader.buffered_len(), 0);
        }
    }

    #[test]
    fn reply_copy_budget_failure_is_reported_as_overloaded_to_all_waiters() {
        let (client, mut server) = std::os::unix::net::UnixStream::pair().unwrap();
        client.set_nonblocking(true).unwrap();
        let pool = Rc::new(RefCell::new(CreditPool::from_budgets(
            crate::coord::ClientBudgets {
                max_ops: 2,
                max_bytes: 80,
                node_max_ops: 2,
                node_max_bytes: 80,
            },
        )));
        let conn = Rc::new(MuxConn::new_accounted(
            "127.0.0.1:1".parse().unwrap(),
            7,
            pool.clone(),
        ));
        {
            let mut state = conn.st.borrow_mut();
            state.fd = client.as_raw_fd();
            state.sock = Some(super::super::Sock::Unix(client));
            state.set_negotiated_for_test();
            for id in [11, 12] {
                let credit = try_transport_credit(&pool, 7, 1, 0).unwrap();
                state.pending.insert(
                    id,
                    Waiter {
                        op_credit: Some(credit),
                        send_watermark: Some(0),
                        ..Default::default()
                    },
                );
            }
            state.driver = Some(11);
        }
        let frame = frame_msg(11, &[5u8; 48]).unwrap();
        server.write_all(&frame).unwrap();
        let mut first = Box::pin(MuxRpc {
            conn: conn.clone(),
            payload: None,
            op_credit: None,
            request_credit: None,
            id: 11,
            queued: true,
            send_only: false,
            after_idle: false,
        });
        let mut second = Box::pin(MuxRpc {
            conn: conn.clone(),
            payload: None,
            op_credit: None,
            request_credit: None,
            id: 12,
            queued: true,
            send_only: false,
            after_idle: false,
        });
        let waker = Waker::noop().clone();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(
            first.as_mut().poll(&mut cx),
            Poll::Ready(Err(TransportError::Overloaded))
        ));
        assert!(matches!(
            second.as_mut().poll(&mut cx),
            Poll::Ready(Err(TransportError::Overloaded))
        ));
        assert_eq!(pool.borrow().stats().in_flight_ops, 0);
        assert_eq!(pool.borrow().stats().in_flight_bytes, 0);
    }

    #[test]
    fn retained_scan_page_and_read_winner_cannot_deadlock_continuation_rpcs() {
        let config = ClientConfig {
            connect_deadline: Some(Duration::from_millis(10)),
            rpc_deadline: Some(Duration::from_millis(1)),
            budgets: Some(crate::coord::ClientBudgets {
                max_ops: 2,
                max_bytes: 256,
                node_max_ops: 2,
                node_max_bytes: 256,
            }),
        };
        let addr = "127.0.0.1:1".parse().unwrap();
        let transport = NetTransport::connect_with_config(&[(1, addr)], config).unwrap();

        let retained_page = Transport::retain_response(&transport, 1, 240)
            .unwrap()
            .unwrap();
        let scan = vals_rt::block_on(transport.scan(1, 0, None, None, 0), || {
            reactor::poll_io();
        });
        assert!(matches!(scan, Err(TransportError::Overloaded)));
        assert_eq!(transport.transport_credit_stats().unwrap().waiters, 0);
        drop(retained_page);

        let retained_winner = Transport::retain_response(&transport, 1, 240)
            .unwrap()
            .unwrap();
        let repair = vals_rt::block_on(
            transport.apply(
                1,
                0,
                b"key",
                Some(b"value"),
                crate::hlc::Version { hlc: 1, node: 1 },
            ),
            reactor::poll_io,
        );
        assert!(matches!(repair, Err(TransportError::Overloaded)));
        assert_eq!(transport.transport_credit_stats().unwrap().waiters, 0);
        drop(retained_winner);

        let retained_scalar = Transport::retain_response(&transport, 1, 240)
            .unwrap()
            .unwrap();
        let reads = [MixedReadEntry {
            key: b"key",
            role: MixedReadRole::Value,
        }];
        let mixed = vals_rt::block_on(transport.read_mixed(1, 0, &reads), || {
            reactor::poll_io();
        });
        assert!(matches!(mixed.as_slice(), [Err(TransportError::Overloaded)]));
        assert_eq!(transport.transport_credit_stats().unwrap().waiters, 0);
        drop(retained_scalar);

        let stats = transport.transport_credit_stats().unwrap();
        assert_eq!(stats.in_flight_ops, 0);
        assert_eq!(stats.in_flight_bytes, 0);
    }

    #[test]
    fn response_heavy_anti_entropy_and_control_calls_hit_the_byte_limit() {
        let config = ClientConfig {
            connect_deadline: Some(Duration::from_secs(1)),
            rpc_deadline: Some(Duration::from_secs(1)),
            budgets: Some(crate::coord::ClientBudgets {
                max_ops: 8,
                max_bytes: 2000,
                node_max_ops: 8,
                node_max_bytes: 2000,
            }),
        };

        let (addr, server) = encoded_response_server(Response::Tree(
            (0..100).map(|i| (i, i as u64)).collect(),
        ));
        let transport = NetTransport::connect_with_config(&[(1, addr)], config).unwrap();
        let tree = vals_rt::block_on(transport.peer(1).tree(0, 0, 0, &[], None), || {
            reactor::poll_io();
        });
        assert!(matches!(tree, Err(TransportError::Overloaded)));
        assert!(transport.transport_credit_stats().unwrap().in_flight_bytes <= 2000);
        drop(transport);
        server.join().unwrap();

        let (addr, server) = encoded_response_server(Response::Members {
            epoch: 1,
            vnodes: 1,
            rf: 1,
            members: (0..200).map(|i| (i, 1)).collect(),
            addrs: Vec::new(),
        });
        let transport = NetTransport::connect_with_config(&[(1, addr)], config).unwrap();
        let members = vals_rt::block_on(transport.members(1), reactor::poll_io);
        assert!(matches!(members, Err(TransportError::Overloaded)));
        assert!(transport.transport_credit_stats().unwrap().in_flight_bytes <= 2000);
        drop(transport);
        server.join().unwrap();
    }

    #[test]
    fn decoded_response_accounting_covers_every_heap_owning_variant() {
        let version = crate::hlc::Version { hlc: 1, node: 2 };
        let responses = [
            Response::Tree(vec![(1, 2)]),
            Response::BucketDigests {
                digests: vec![(vec![1], version)],
                more: true,
                resume: Some(vec![2]),
            },
            Response::Members {
                epoch: 1,
                vnodes: 1,
                rf: 1,
                members: vec![(1, 1)],
                addrs: vec![(1, vec!["127.0.0.1:1".parse().unwrap()])],
            },
            Response::Zones {
                epoch: 1,
                zones: vec![wire::OwnedZone {
                    id: 1,
                    rf: 1,
                    ordering: 0,
                    scope: Some(vec![1]),
                    name: "zone".into(),
                }],
            },
            Response::RangeMaps {
                epoch: 1,
                maps: vec![wire::OwnedRangeMap {
                    zone_id: 1,
                    splits: vec![vec![1]],
                    tokens: vec![vec![2]],
                    pending: vec![Some(vec![3])],
                    placement_debt: vec![true],
                    opaque_tokens: true,
                }],
            },
            Response::RangeStats {
                epoch: 1,
                ranges: vec![wire::OwnedRangeStat {
                    zone_id: 1,
                    lower_bound: vec![1],
                    entries: 1,
                    midpoint: Some(vec![2]),
                    ops_per_sec: 0,
                    shard_index: 0,
                    shard_count: 1,
                    rehome_mirror_epoch: 0,
                    rehome_mirror_ok: 0,
                    rehome_mirror_failures: 0,
                    rehome_copy_complete_epoch: 0,
                    rehome_copy_progress: 0,
                }],
            },
            Response::Batch(vec![Applied::Won]),
            Response::Err("error".into()),
        ];
        for response in responses {
            assert!(transport::response_owned_bytes(&response) > 0, "{response:?}");
        }
    }

    #[test]
    fn incompatible_server_receives_no_request_bytes() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let _hello = read_frame(&mut stream);
            let mut old = Vec::new();
            old.extend_from_slice(&crate::proto::HELLO_MAGIC);
            old.extend_from_slice(&(crate::proto::MIN_PROTO_VERSION - 1).to_le_bytes());
            old.extend_from_slice(&crate::proto::FEATURES.to_le_bytes());
            stream.write_all(&(old.len() as u32).to_le_bytes()).unwrap();
            stream.write_all(&old).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            let mut byte = [0u8; 1];
            assert_eq!(stream.read(&mut byte).unwrap(), 0);
        });
        let conn = Rc::new(MuxConn::new(addr));
        let result = vals_rt::block_on(MuxRpc::new(conn, b"must-not-send".to_vec()), || {
            flush_dirty();
            reactor::poll_io();
        });
        assert!(matches!(result, Err(TransportError::Unavailable)));
        server.join().unwrap();
    }

    fn serve_one(stream: &mut std::net::TcpStream, response: &Response) -> Vec<u8> {
        let request = read_frame(stream);
        let id = u64::from_le_bytes(request[..8].try_into().unwrap());
        let payload = wire::encode_response_payload(response);
        write_frame(stream, &frame_msg(id, &payload).unwrap());
        request[8..].to_vec()
    }

    fn idle_close_server(redial: bool) -> (SocketAddr, std::thread::JoinHandle<Option<Vec<u8>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut first, _) = listener.accept().unwrap();
            let _hello = read_frame(&mut first);
            first.write_all(&crate::proto::encode_hello()).unwrap();
            serve_one(&mut first, &Response::Read(None));
            let _unanswered = read_frame(&mut first);
            drop(first);
            if !redial {
                return None;
            }
            let (mut second, _) = listener.accept().unwrap();
            let _hello = read_frame(&mut second);
            second.write_all(&crate::proto::encode_hello()).unwrap();
            let stored = Stored {
                version: crate::hlc::Version { hlc: 7, node: 1 },
                value: Some(b"fresh".to_vec()),
            };
            let retried = serve_one(&mut second, &Response::Read(Some(stored)));
            let _ = second.read(&mut [0u8; 1]);
            Some(retried)
        });
        (addr, server)
    }

    fn mark_idle(transport: &NetTransport) {
        transport.conn(1, 0).unwrap().st.borrow_mut().last_io =
            Instant::now().checked_sub(IDLE_REUSE_AFTER * 2);
    }

    fn reconnects(transport: &NetTransport) -> u64 {
        transport.connection_stats()[0].reconnects
    }

    #[test]
    fn idle_closed_connection_read_is_retried_once_on_a_fresh_connection() {
        let (addr, server) = idle_close_server(true);
        let transport = NetTransport::connect(&[(1, addr)]);
        assert_eq!(transport.rpc_attempts(), 0);
        let first = vals_rt::block_on(transport.read(1, 0, b"key"), reactor::poll_io);
        assert_eq!(first, Ok(None));
        assert_eq!(transport.rpc_attempts(), 1);
        mark_idle(&transport);
        let retries = crate::observe::snapshot().idle_conn_retries;
        let second = vals_rt::block_on(transport.read(1, 0, b"key"), reactor::poll_io);
        assert_eq!(second.unwrap().unwrap().value.as_deref(), Some(&b"fresh"[..]));
        assert_eq!(transport.rpc_attempts(), 3);
        assert_eq!(reconnects(&transport), 1);
        assert!(crate::observe::snapshot().idle_conn_retries > retries);
        drop(transport);
        let retried = wire::decode_request(&server.join().unwrap().unwrap()).unwrap();
        assert!(matches!(retried, wire::OwnedRequest::Read { ref key, .. } if key == b"key"));
    }

    #[test]
    fn idle_closed_connection_write_is_not_retried() {
        let (addr, server) = idle_close_server(false);
        let transport = NetTransport::connect(&[(1, addr)]);
        let first = vals_rt::block_on(transport.read(1, 0, b"key"), reactor::poll_io);
        assert_eq!(first, Ok(None));
        mark_idle(&transport);
        let version = crate::hlc::Version { hlc: 1, node: 1 };
        let write = vals_rt::block_on(
            transport.apply(1, 0, b"key", Some(b"value"), version),
            reactor::poll_io,
        );
        assert_eq!(write, Err(TransportError::Unavailable));
        assert_eq!(reconnects(&transport), 0);
        assert!(server.join().unwrap().is_none());
    }

    #[test]
    fn busy_connection_read_failure_is_not_retried() {
        let (addr, server) = idle_close_server(false);
        let transport = NetTransport::connect(&[(1, addr)]);
        let first = vals_rt::block_on(transport.read(1, 0, b"key"), reactor::poll_io);
        assert_eq!(first, Ok(None));
        let second = vals_rt::block_on(transport.read(1, 0, b"key"), reactor::poll_io);
        assert_eq!(second, Err(TransportError::Unavailable));
        assert_eq!(reconnects(&transport), 0);
        assert!(server.join().unwrap().is_none());
    }
