extern crate actix_web;

#[macro_use]
extern crate tantivy;
extern crate failure;

#[macro_use]
extern crate serde_json;

use actix_web::{middleware, server, App, HttpRequest, HttpResponse};
use failure::ResultExt;

pub mod err {
    pub use ::failure::err_msg as msg;
    pub type Result<T> = ::std::result::Result<T, ::failure::Error>;
}

pub mod search {
    pub use tantivy::{
        collector::*,
        query::{AllQuery, BooleanQuery, Occur, Query, QueryParser, TermQuery},
        schema::*,
        Index, Term
    };

    use super::err;

    pub fn create_index() -> err::Result<Index> {
        let mut schema_builder = SchemaBuilder::default();

        schema_builder.add_i64_field("id", INT_INDEXED | INT_STORED);
        schema_builder.add_text_field("title", TEXT | STORED);
        schema_builder.add_facet_field("facets");

        let schema = schema_builder.build();
        let index = Index::create_in_dir("idx", schema.clone())?;

        let mut index_writer = index.writer(100_000_000)?;

        for id in 1..10001 {

            let facets = vec![
                format!("/subjects/id_{}", id),
                format!("/subjects/id_mod_2_{}", id % 2),
                format!("/subjects/id_mod_99_{}", id % 99),
            ];

            let json = json!({
                "id": id, 
                "title": "test",
                "facets": facets
            }).to_string();

            index_writer.add_document(schema.parse_document(&json).map_err(::tantivy::Error::from)?);
        }

        index_writer.commit()?;

        index.load_searchers()?;

        Ok(index)
    }

    pub fn open_index() -> err::Result<Index> {
        let index = Index::open_in_dir("idx")?;
        Ok(index)
    }

    pub fn subjects(index: &Index) -> err::Result<Vec<::serde_json::Value>> {
        let schema = index.schema();

        let searcher = index.searcher();

        let facets_field = schema.get_field("facets").unwrap();

        let mut top_facets = FacetCollector::for_field(facets_field);

        top_facets.add_facet("/subjects");

        searcher.search(&AllQuery, &mut top_facets)?;

        let harvested_facets = top_facets.harvest();

        let mut subjects = harvested_facets
            .get("/subjects")
            .into_iter()
            .map(|(facet, doc_count)| json!({ 
                "key": facet.to_string().split('/').collect::<Vec<_>>()[1..].join("|"), 
                "doc_count": doc_count
            }))
            .collect::<Vec<_>>();

        subjects.sort_by_key(|j| j["doc_count"].as_i64() );
        subjects.reverse();

        Ok(subjects)
    }

    pub fn update_facets_for_doc_1(index: &Index, facets: Vec<String>) -> err::Result<()> {
        let schema = index.schema();

        let mut index_writer = index.writer(50_000_000)?;

        index_writer
            .delete_term(Term::from_field_i64(schema.get_field("id").unwrap(), 1i64));

        let json = json!({
            "id": 1, 
            "title": "who fished alone in a skiff",
            "facets": facets
        }).to_string();

        let doc = schema.parse_document(&json).map_err(::tantivy::Error::from)?;

        index_writer.add_document(doc);

        index_writer.commit()?;

        index.load_searchers()?;

        Ok(())
    }


}

fn root(_req: HttpRequest<State>) -> Result<HttpResponse, ::actix_web::Error> {
    let body = r#"
    <p><a href="/">/</a></p>
    <p><a href="/subjects">/subjects</a></p>
    <p><form action="/docs/1/update" method="POST"><button>/docs/1/update</button></form></p>
    <p><form action="/docs/1/restore" method="POST"><button>/docs/1/restore</button></form></p>"#;

    Ok(HttpResponse::Ok().content_type("text/html").body(body))
}


fn subjects(req: HttpRequest<State>) -> Result<HttpResponse, ::actix_web::Error> {
    let subjects = search::subjects(&req.state().index)?;

    let body = ::serde_json::to_string_pretty(&subjects).compat()?;

    Ok(HttpResponse::Ok().content_type("application/json").body(body))
}

fn doc_1_update(req: HttpRequest<State>) -> Result<HttpResponse, ::actix_web::Error> {
    let facets = vec![
        "/subjects/id_0".into(),
        "/subjects/id_mod_2_0".into(),
        "/subjects/id_mod_99_0".into(),
        "/subjects/something_else".into()
    ];

    search::update_facets_for_doc_1(&req.state().index, facets)?;

    Ok(HttpResponse::Found().header("LOCATION", "/").finish())
}

fn doc_1_restore(req: HttpRequest<State>) -> Result<HttpResponse, ::actix_web::Error> {
    let facets = vec![
        "/subjects/id_1".into(),
        "/subjects/id_mod_2_1".into(),
        "/subjects/id_mod_99_1".into()
    ];

    search::update_facets_for_doc_1(&req.state().index, facets)?;

    Ok(HttpResponse::Found().header("LOCATION", "/").finish())
}


pub struct State {
    index: search::Index
}

fn main() -> err::Result<()> {

    if !::std::path::Path::new("idx/meta.json").exists() {
        search::create_index()?;
    }

    let index = || {
        println!("Starting up");
        search::open_index().expect("Couldn't get index")
    };
    
    server::new(move || {

        App::with_state(State { index: index() })
            .middleware(middleware::Logger::default())
            .resource("/", |r| r.get().with(root))
            .resource("/subjects", |r| r.get().with(subjects))
            .resource("/docs/1/update", |r| r.post().with(doc_1_update))
            .resource("/docs/1/restore", |r| r.post().with(doc_1_restore))
    }).bind("0.0.0.0:3000")?
        .run();

    Ok(())
}
