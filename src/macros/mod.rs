mod admin;
mod build_info;
mod config;
mod debug;
mod implement;
mod refutable;
mod utils;

use proc_macro::TokenStream;
use syn::{
	Error, Item, ItemEnum, ItemFn, ItemStruct, Meta,
	parse::{Parse, Parser},
	parse_macro_input,
};

pub(crate) type Result<T> = std::result::Result<T, Error>;

#[proc_macro_attribute]
pub fn admin_command(args: TokenStream, input: TokenStream) -> TokenStream {
	attribute_macro::<ItemFn, _>(args, input, admin::command)
}

#[proc_macro_attribute]
pub fn admin_command_dispatch(args: TokenStream, input: TokenStream) -> TokenStream {
	attribute_macro::<ItemEnum, _>(args, input, admin::command_dispatch)
}

#[proc_macro_attribute]
pub fn recursion_depth(args: TokenStream, input: TokenStream) -> TokenStream {
	attribute_macro::<Item, _>(args, input, debug::recursion_depth)
}

#[proc_macro_attribute]
pub fn refutable(args: TokenStream, input: TokenStream) -> TokenStream {
	attribute_macro::<ItemFn, _>(args, input, refutable::refutable)
}

#[proc_macro_attribute]
pub fn implement(args: TokenStream, input: TokenStream) -> TokenStream {
	attribute_macro::<ItemFn, _>(args, input, implement::implement)
}

#[proc_macro_attribute]
pub fn config_example_generator(args: TokenStream, input: TokenStream) -> TokenStream {
	attribute_macro::<ItemStruct, _>(args, input, config::example_generator)
}

/// Runs an async test body on the smol executor without requiring a runtime
/// specific test attribute.
#[proc_macro_attribute]
pub fn async_test(_args: TokenStream, input: TokenStream) -> TokenStream {
	let mut function = parse_macro_input!(input as ItemFn);
	function.sig.asyncness = None;
	function.attrs.push(syn::parse_quote!(#[test]));
	let body = function.block;
	function.block = Box::new(syn::parse_quote!({
		smol::block_on(async move #body)
	}));
	quote::quote!(#function).into()
}

#[proc_macro]
pub fn introspect_crate(input: TokenStream) -> TokenStream {
	build_info::introspect(input.into())
		.unwrap_or_else(|e| e.to_compile_error())
		.into()
}

fn attribute_macro<I, F>(args: TokenStream, input: TokenStream, func: F) -> TokenStream
where
	F: Fn(I, &[Meta]) -> Result<TokenStream>,
	I: Parse,
{
	let item = parse_macro_input!(input as I);
	syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated
		.parse(args)
		.map(|args| args.iter().cloned().collect::<Vec<_>>())
		.and_then(|ref args| func(item, args))
		.unwrap_or_else(|e| e.to_compile_error().into())
}
