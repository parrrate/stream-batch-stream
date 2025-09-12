use futures_util::{Stream, TryStream, TryStreamExt};
use genawaiter::sync::Gen;

pub fn stream_batch_stream<K, V, S: TryStream<Ok = (K, V)>>(
    mut key: K,
    mut next: impl FnMut(&K) -> Result<K, S::Error>,
    mut fetch: impl FnMut(K) -> S,
) -> impl Stream<Item = Result<(K, V), S::Error>> {
    Gen::new(async move |co| {
        let r: Result<(), S::Error> = async {
            loop {
                let batch = fetch(key).try_collect::<Vec<_>>().await?;
                let Some((last, _)) = batch.last() else {
                    break Ok(());
                };
                key = next(last)?;
                for item in batch {
                    co.yield_(Ok(item)).await;
                }
            }
        }
        .await;
        if let Err(e) = r {
            co.yield_(Err(e)).await;
        }
    })
}
