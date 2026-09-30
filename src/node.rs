//! Node compatibility's services, for one worker.
//!
//! Deno's Node layer (`node:` built-ins, `require`, `process`) reads a resolver, a package.json
//! reader, a require loader and the system interface from the op state, and an op that finds one
//! missing PANICS, inside a V8 callback that cannot unwind: the whole runtime aborts, every
//! project on the host with it. Measured 2026-09-28: `npm:stripe`'s first `require` did exactly
//! that. So every worker gets all of them, even though a prepared bundle has its npm packages
//! inlined and no `node_modules` to resolve against: "bring your own node_modules" mode with none
//! brought, rooted at the bundle, which may read nothing outside it.

use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;

use deno_ast::MediaType;
use deno_core::FastString;
use deno_error::JsErrorBox;
use deno_resolver::npm::{ByonmNpmResolverCreateOptions, CreateInNpmPkgCheckerOptions, DenoInNpmPackageChecker, NpmResolver, NpmResolverCreateOptions};
use deno_runtime::deno_node::{NodeExtInitServices, NodeRequireLoader};
use deno_runtime::deno_permissions::PermissionsContainer;
use node_resolver::cache::NodeResolutionSys;
use node_resolver::errors::PackageJsonLoadError;
use node_resolver::{DenoIsBuiltInNodeModuleChecker, NodeResolver, NodeResolverOptions, PackageJsonResolver};
use url::Url;

use crate::loader::{Root, strip_types};

pub type Sys = sys_traits::impls::RealSys;

pub fn services(root: Root) -> NodeExtInitServices<DenoInNpmPackageChecker, NpmResolver<Sys>, Sys> {
	let sys = Sys::default();
	let pkg_json_resolver = Arc::new(PackageJsonResolver::new(sys.clone(), None));
	let resolution_sys = NodeResolutionSys::new(sys.clone(), None);
	let npm_resolver = NpmResolver::<Sys>::new::<Sys>(NpmResolverCreateOptions::Byonm(ByonmNpmResolverCreateOptions {
		sys: resolution_sys.clone(),
		pkg_json_resolver: pkg_json_resolver.clone(),
		root_node_modules_dir: None,
		// No node_modules anywhere: a prepared bundle has its packages inlined.
		search_stop_dir: Some(PathBuf::from("/")),
	}));
	let node_resolver = Arc::new(NodeResolver::new(
		DenoInNpmPackageChecker::new(CreateInNpmPkgCheckerOptions::Byonm),
		DenoIsBuiltInNodeModuleChecker,
		npm_resolver,
		pkg_json_resolver.clone(),
		resolution_sys,
		NodeResolverOptions::default(),
	));
	NodeExtInitServices {
		node_require_loader: Rc::new(RequireLoader { root }),
		node_resolver,
		pkg_json_resolver,
		sys,
	}
}

/// `require()` of a file: only ever one in this worker's own bundle.
struct RequireLoader {
	root: Root,
}

impl NodeRequireLoader for RequireLoader {
	fn ensure_read_permission<'a>(&self, _permissions: &mut PermissionsContainer, path: Cow<'a, Path>) -> Result<Cow<'a, Path>, JsErrorBox> {
		if path.starts_with(&*self.root.borrow()) && !path.components().any(|c| matches!(c, std::path::Component::ParentDir)) {
			Ok(path)
		} else {
			Err(JsErrorBox::new("NotCapable", format!("{} is outside this function's bundle", path.display())))
		}
	}

	fn load_text_file_lossy(&self, path: &Path) -> Result<FastString, JsErrorBox> {
		let bytes = std::fs::read(path).map_err(JsErrorBox::from_err)?;
		let text = String::from_utf8_lossy(&bytes).into_owned();
		let media_type = MediaType::from_path(path);
		let text = match media_type {
			MediaType::TypeScript | MediaType::Mts | MediaType::Cts | MediaType::Tsx | MediaType::Jsx => {
				let specifier = Url::from_file_path(path).map_err(|()| JsErrorBox::generic("not an absolute path"))?;
				strip_types(&specifier, text, media_type)?
			}
			_ => text,
		};
		Ok(text.into())
	}

	fn is_maybe_cjs(&self, specifier: &Url) -> Result<bool, PackageJsonLoadError> {
		Ok(matches!(MediaType::from_specifier(specifier), MediaType::Cjs | MediaType::Cts))
	}

	fn is_maybe_cjs_from_require(&self, specifier: &Url) -> Result<bool, PackageJsonLoadError> {
		Ok(matches!(MediaType::from_specifier(specifier), MediaType::Cjs | MediaType::Cts | MediaType::JavaScript))
	}
}
