use std::{convert::Infallible, env, path::Path, sync::Arc, time::Duration};

use asynk_strim::{Yielder, stream_fn};
use axum::{
    Router,
    extract::State,
    response::{Html, IntoResponse, Sse, sse::Event},
    routing::get,
};
use datastar::patch_elements::PatchElements;
use image::load_from_memory;
use image_ascii::TextGenerator;
use tokio::{
    net::TcpListener,
    spawn,
    sync::{Notify, RwLock},
    time::sleep,
};
use tower_http::compression::{CompressionLayer, predicate::SizeAbove};
use vid2img::FileSource;

struct AppState {
    frame: RwLock<String>,
    frame_notify: Notify,
}

#[tokio::main]
async fn main() {
    let video_path: String = env::args()
        .collect::<Vec<String>>()
        .get(1)
        .expect("video path required")
        .into();

    let mut frames = Vec::new();
    let frame_source = FileSource::new(Path::new(&video_path), (480, 360)).unwrap();
    for frame in frame_source.into_iter() {
        if let Ok(Some(data)) = frame {
            let image = load_from_memory(&data).unwrap();
            let result: String = TextGenerator::new(&image).generate();
            frames.push(result);
            println!("Loading frame...");
        }
    }

    let state = Arc::new(AppState {
        frame: RwLock::new(frames.pop().unwrap().to_string()),
        frame_notify: Notify::new(),
    });

    let app = Router::new()
        .route("/", get(root))
        .route("/frame", get(frame))
        .with_state(state.clone())
        .layer(
            CompressionLayer::new()
                .br(true)
                .compress_when(SizeAbove::default()),
        );

    let host_port = "127.0.0.1:3000";
    let listener = TcpListener::bind(host_port).await.unwrap();

    spawn(async move {
        loop {
            let result = frames.pop();

            if let Some(new_frame) = result {
                let mut frame = state.frame.write().await;
                *frame = new_frame;
                state.frame_notify.notify_waiters();
            }

            sleep(Duration::from_secs_f32(1.0 / 24.0)).await;
        }
    });

    println!("Listening on {}", host_port);
    axum::serve(listener, app).await.unwrap();
}

async fn root() -> impl IntoResponse {
    Html(
        r#"
    <script type="module" src="https://cdn.jsdelivr.net/gh/starfederation/datastar@v1.0.4/bundles/datastar.js"></script>
    <div id="frame" data-init="@get('/frame')">Loading...</div>
    "#,
    )
}

async fn frame(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    Sse::new(stream_fn(
        move |mut yielder: Yielder<Result<Event, Infallible>>| async move {
            loop {
                state.frame_notify.notified().await;

                let event = PatchElements::new(format!(
                    r#"
                    <div id="frame"><pre>{}</pre></div>
                    "#,
                    state.frame.read().await
                ));

                yielder
                    .yield_item(Ok(event.write_as_axum_sse_event()))
                    .await;
            }
        },
    ))
}
