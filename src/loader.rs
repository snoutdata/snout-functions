//! Loading a bundle's modules into an isolate.
//!
//! A worker may load files from its own bundle's directory and nothing else: the resolved URL
//! must stay under that directory after `..` is taken out, or the load is refused. TypeScript is
//! stripped of its types here (deno_ast, no type check, like `deno run --no-check`).
//!
//! Remote (`https:`), `jsr:` and `npm:` imports are resolved when a bundle is PREPARED, never on
//! a customer's request: the host resolves them into `.built/main.js` when a bundle arrives. A
//! bundle that has not been prepared is refused with a sentence saying so.

use std::borrow::Cow;
use std::cell::RefCell;
use std::path::PathBuf;
use std::rc::Rc;

use deno_ast::{MediaType, ParseParams, SourceMapOption};
use deno_core::error::ModuleLoaderError;
use deno_core::{
	ModuleLoadOptions, ModuleLoadReferrer, ModuleLoadResponse, ModuleLoader, ModuleSource, ModuleSourceCode,
	ModuleSpecifier, ModuleType, ResolutionKind, resolve_import,
};
use deno_error::JsErrorBox;

/// The bundle directory a worker may load from. Shared with the require loader (node.rs) and
/// bound when a pre-booted isolate is CLAIMED for a function (isolate.rs): until then it names a
/// directory that does not exist, so nothing resolves.
pub type Root = Rc<RefCell<PathBuf>>;

pub fn unbound_root() -> Root {
	Rc::new(RefCell::new(PathBuf::from("/nonexistent/unclaimed")))
}

pub struct BundleLoader {
	root: Root,
}

impl BundleLoader {
	pub fn new(root: Root) -> Self {
		BundleLoader { root }
	}

	fn inside(&self, specifier: &ModuleSpecifier) -> Result<PathBuf, ModuleLoaderError> {
		let path = specifier
			.to_file_path()
			.map_err(|_| JsErrorBox::generic(format!("{specifier} is not a file in this function's bundle")))?;
		// `to_file_path` has already folded `..`, so a path outside the root is visible here.
		if !path.starts_with(&*self.root.borrow()) {
			return Err(JsErrorBox::generic(format!("{specifier} is outside this function's bundle")));
		}
		Ok(path)
	}
}

impl ModuleLoader for BundleLoader {
	fn resolve(&self, specifier: &str, referrer: &str, _kind: ResolutionKind) -> Result<ModuleSpecifier, ModuleLoaderError> {
		let resolved = resolve_import(specifier, referrer).map_err(JsErrorBox::from_err)?;
		match resolved.scheme() {
			"file" => {
				self.inside(&resolved)?;
				Ok(resolved)
			}
			// Node's built-ins are modules in the runtime's snapshot, loaded by deno_core itself.
			"node" if deno_runtime::deno_node::is_builtin_node_module(resolved.path()) => Ok(resolved),
			"npm" | "jsr" | "http" | "https" => Err(JsErrorBox::generic(format!(
				"{specifier}: imports from a registry or a URL are resolved when the host prepares a bundle, and this one has not been prepared yet; try again in a moment"
			))),
			other => Err(JsErrorBox::generic(format!("{specifier}: the {other}: scheme is not supported"))),
		}
	}

	fn load(
		&self,
		specifier: &ModuleSpecifier,
		_referrer: Option<&ModuleLoadReferrer>,
		_options: ModuleLoadOptions,
	) -> ModuleLoadResponse {
		ModuleLoadResponse::Sync(self.load_now(specifier))
	}

	fn get_source_map(&self, _specifier: &str) -> Option<Cow<'_, [u8]>> {
		None
	}
}

impl BundleLoader {
	fn load_now(&self, specifier: &ModuleSpecifier) -> Result<ModuleSource, ModuleLoaderError> {
		let path = self.inside(specifier)?;
		let media_type = MediaType::from_path(&path);
		let (module_type, transpile) = match media_type {
			MediaType::JavaScript | MediaType::Mjs => (ModuleType::JavaScript, false),
			MediaType::Jsx | MediaType::TypeScript | MediaType::Mts | MediaType::Tsx => (ModuleType::JavaScript, true),
			MediaType::Json => (ModuleType::Json, false),
			_ => {
				return Err(JsErrorBox::generic(format!("{specifier}: this kind of file cannot be imported")));
			}
		};
		let text = std::fs::read_to_string(&path)
			.map_err(|error| JsErrorBox::generic(format!("{specifier} could not be read: {error}")))?;
		let code = if transpile { strip_types(specifier, text, media_type)? } else { text };
		Ok(ModuleSource::new(module_type, ModuleSourceCode::String(code.into()), specifier, None))
	}
}

/// TypeScript to JavaScript, types removed, nothing checked.
pub fn strip_types(specifier: &ModuleSpecifier, text: String, media_type: MediaType) -> Result<String, ModuleLoaderError> {
	let parsed = deno_ast::parse_module(ParseParams {
		specifier: specifier.clone(),
		text: text.into(),
		media_type,
		capture_tokens: false,
		scope_analysis: false,
		maybe_syntax: None,
	})
	.map_err(JsErrorBox::from_err)?;
	let emitted = parsed
		.transpile(
			&deno_ast::TranspileOptions {
				imports_not_used_as_values: deno_ast::ImportsNotUsedAsValues::Remove,
				decorators: deno_ast::DecoratorsTranspileOption::Ecma,
				..Default::default()
			},
			&deno_ast::TranspileModuleOptions { module_kind: None },
			&deno_ast::EmitOptions { source_map: SourceMapOption::Inline, inline_sources: true, ..Default::default() },
		)
		.map_err(JsErrorBox::from_err)?;
	Ok(emitted.into_source().text)
}
