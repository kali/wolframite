use std::collections::{ BTreeSet, BTreeMap };

#[allow(dead_code)]
fn lineage(url:&str) -> Vec<String> {
    let mut result = vec!();
    let splits:Vec<&str> = url.split("/").collect();
    let mut prefix = splits[0..1].connect("/");
    for token in splits[1..].iter() {
        prefix.push('/');
        prefix.push_str(token);
        result.push(prefix.clone());
    }
    result
}

#[allow(dead_code)]
pub fn aggregate_urls(set:&BTreeSet<String>) -> Vec<(String,usize, usize)> {
    let mut result:BTreeMap<String,(usize,usize)> = BTreeMap::new();
    for url in set {
        let mut lineage = lineage(&*url);
        if lineage.len() > 0 {
            lineage.pop();
            let last_parent = lineage.last().unwrap().clone();
            let previous = *result.get(&*last_parent).unwrap_or(&(0usize,0usize));
            result.insert(last_parent, (previous.0, previous.1 + 1));
            for parent in lineage {
                let previous = *result.get(&*parent).unwrap_or(&(0usize,0usize));
                result.insert(parent, (previous.0 + 1, previous.1));
            }
        }
    };
    result.iter().filter( |&(_,v)| -> bool {
        v.0 > 1 && v.0 > (set.len() / 1000) && v.1 > v.0 / 2
    }).map(|(k,v)| -> (String,usize,usize) { (k.clone(),v.0,v.1) }).collect()
}

#[test]
fn test_url_aggregator() {
    let mut set = BTreeSet::new();
    set.insert("http://a.b.c".to_string());
    set.insert("http://a.b.c/".to_string());
    set.insert("http://a.b.c/a".to_string());
    set.insert("http://a.b.c/a/b/b".to_string());
    set.insert("http://a.b.c/a/b/c/d".to_string());
    set.insert("http://a.b.c/a/b/c/e".to_string());
    set.insert("http://a.b.c/a/b/c/f/h".to_string());
    set.insert("http://a.b.c/a/b/c/g".to_string());
    set.insert("http://a.b.c/a/c".to_string());
    set.insert("http://a.b.c/a/d".to_string());
    set.insert("http://d.e.f/a/d".to_string());

    let result = aggregate_urls(&set);

    assert_eq!(1, result.len());
}
