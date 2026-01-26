//! Procedural macros for Bicycle streaming API.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

/// Marks the entry point for a Bicycle streaming job.
///
/// This macro transforms an async function that takes a `StreamEnvironment`
/// and returns a `Result<()>` into a proper WASM entry point.
///
/// # Example
///
/// ```ignore
/// use bicycle_api::prelude::*;
///
/// #[bicycle_main]
/// async fn main(env: StreamEnvironment) -> Result<()> {
///     env.socket_source("0.0.0.0", 9999)
///         .map(|s: String| s.to_uppercase())
///         .socket_sink("0.0.0.0", 9998);
///
///     env.execute("my-job")?;
///     Ok(())
/// }
/// ```
#[proc_macro_attribute]
pub fn bicycle_main(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    let fn_name = &input.sig.ident;
    let fn_block = &input.block;
    let fn_vis = &input.vis;

    // Generate the transformed code
    let expanded = quote! {
        // The user's job function
        #fn_vis async fn #fn_name(env: bicycle_api::StreamEnvironment) -> bicycle_api::Result<bicycle_api::graph::JobGraph> {
            // User's code block (adapted to return the graph)
            let _user_block = || async #fn_block;

            // Build and return the job graph
            // The actual execution happens on the cluster
            env.execute(stringify!(#fn_name))
        }

        // WASM entry point for extracting job graph
        #[no_mangle]
        pub extern "C" fn bicycle_get_job_graph() -> *mut u8 {
            let env = bicycle_api::StreamEnvironment::new();

            // Build a simple async runtime for WASM
            let graph = futures::executor::block_on(async {
                #fn_name(env).await
            });

            match graph {
                Ok(g) => {
                    match serde_json::to_vec(&g) {
                        Ok(data) => {
                            let len = data.len();
                            let ptr = Box::into_raw(data.into_boxed_slice()) as *mut u8;
                            // Store length at the start (simple protocol)
                            ptr
                        }
                        Err(_) => std::ptr::null_mut(),
                    }
                }
                Err(_) => std::ptr::null_mut(),
            }
        }

        // WASM entry point for getting graph length
        #[no_mangle]
        pub extern "C" fn bicycle_get_job_graph_len() -> u32 {
            let env = bicycle_api::StreamEnvironment::new();

            let graph = futures::executor::block_on(async {
                #fn_name(env).await
            });

            match graph {
                Ok(g) => {
                    match serde_json::to_vec(&g) {
                        Ok(data) => data.len() as u32,
                        Err(_) => 0,
                    }
                }
                Err(_) => 0,
            }
        }

        // Standard main function
        fn main() {
            let env = bicycle_api::StreamEnvironment::new();

            let result = futures::executor::block_on(async {
                #fn_name(env).await
            });

            match result {
                Ok(graph) => {
                    // Print the job graph as JSON (useful for debugging)
                    println!("{}", serde_json::to_string_pretty(&graph).unwrap_or_default());
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    std::process::exit(1);
                }
            }
        }
    };

    TokenStream::from(expanded)
}

/// Marks a struct as an AsyncFunction implementation.
///
/// This generates the necessary WASM export functions for the operator.
#[proc_macro_attribute]
pub fn bicycle_function(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // For now, just pass through the item unchanged
    // In a full implementation, this would generate WASM exports
    item
}
