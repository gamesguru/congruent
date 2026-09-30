macro_rules! stream_boilerplate {
	(
		map =
		$self:expr,is_cached =
		$is_cached:expr,init =
		$init_method:ident,key =
		$key:expr,dir =
		$dir:expr,stream_type =
		$stream_type:ident
	) => {{
		use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
		use tokio::task;

		use crate::pool::Seek;

		let opts = super::iter_options_default(&$self.db);
		let state = crate::stream::State::new($self, opts);

		let key_slice: Option<&[u8]> = $key;

		if $is_cached {
			let state = state.$init_method(key_slice);
			return task::consume_budget()
				.map(move |()| crate::stream::$stream_type::<'_>::from(state))
				.into_stream()
				.flatten()
				.boxed();
		}

		let seek = Seek {
			map: $self.clone(),
			dir: $dir,
			key: key_slice.map(crate::keyval::KeyBuf::from),
			state: crate::pool::into_send_seek(state),
			res: None,
		};

		$self
			.db
			.pool
			.execute_iter(seek)
			.ok_into::<crate::stream::$stream_type<'_>>()
			.into_stream()
			.try_flatten()
			.boxed()
	}};
}

pub(crate) use stream_boilerplate;
