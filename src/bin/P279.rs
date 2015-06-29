#![allow(non_snake_case)]

extern crate wolframite;
#[macro_use]
extern crate itertools;
extern crate pad;

use std::collections::HashMap;
use std::collections::HashSet;

use itertools::Itertools;

use pad::PadStr;

use wolframite::WikiResult;
use wolframite::wikidata;
use wolframite::wikidata::EntityHelpers;
use wolframite::mapred::MapReduceOp;
use wolframite::wikidata::EntityRef;

fn main() { count().unwrap() }

fn descendency(root:EntityRef, children:&HashMap<EntityRef,Vec<EntityRef>>)
        -> HashSet<EntityRef> {
    let mut result:HashSet<EntityRef> = HashSet::new();
    let mut queue:HashSet<EntityRef> = HashSet::new();
    queue.insert(root);
    while queue.len() > 0 {
        let one = *(queue.iter().next().unwrap());
        if !result.contains(&one) {
            result.insert(one);
            queue.remove(&one);
            for kid in children.get(&one).unwrap_or(&vec!()) {
                if !result.contains(kid) {
                    queue.insert(*kid);
                }
            }
        }
    };
    result.remove(&root);
    result
}

fn count() -> WikiResult<()> {
    let mut wd = wikidata::Wikidata::latest_compiled().unwrap();

    let mro = MapReduceOp::new_map_reduce(
        |e:WikiResult<wikidata::MessageAndEntity>| {
            let e = e.unwrap();
            let v:Vec<((EntityRef,EntityRef),())> = e.get_relations().unwrap()
                .filter(|t| (t.0 == EntityRef::P(279)))
                .map(|t| ((EntityRef::from_id(e.get_id().unwrap()), t.1),()))
                .collect();
            Box::new(v.into_iter())
        },
        |_,_| { () }
    );
    let biter = try!(wd.entity_iter_iter());
    let r = mro.run(biter);

    let mut parents = HashMap::new();
    let mut children = HashMap::new();

    for pair in r.keys() {
        parents.entry(pair.0).or_insert(vec!()).push(pair.1);
        children.entry(pair.1).or_insert(vec!()).push(pair.0);
    }

    let mut roots = HashSet::new();
    for (_,daddies) in parents.iter() {
        for daddy in daddies.iter() {
            if parents.get(daddy).is_none() {
                roots.insert(daddy);
            }
        }
    };
    fn dump_node(depth:usize, wd:&mut wikidata::Wikidata, done:&mut HashSet<EntityRef>,
            children:&HashMap<EntityRef, Vec<EntityRef>>, node:EntityRef) {
        let padding = "".pad_to_width(4*depth);
        let id = node.get_id();
        let label = wd.get_label(&*id).unwrap_or("no label").to_owned();
        let full_label = format!("<b>{}</b> {}", &*id, label);
        println!("{}<li>", padding);
        if done.contains(&node) {
            println!("{}  <a href='#{}'>{}</a>", padding, &*id, full_label);
        } else {
            println!("{}  <a anchor='{}'/>{}", padding, &*id, full_label);
            done.insert(node);
            children.get(&node).map(|kids| {
                kids.iter().foreach(|kid| {
                    println!("{}  <ul>", padding);
                    dump_node(depth+1, wd, done, children, *kid);
                    println!("{}  </ul>", padding);
                });
            });
        }
        println!("{}</li>", padding);
    }

    let mut done = HashSet::new();
    for root in roots {
        dump_node(0, &mut wd, &mut done, &children, *root);
    }
    Ok( () )
}

