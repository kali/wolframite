@0x9ae0d1f47c29ffc8;

struct Page {
    id @0: UInt64;
    title @1: Text;
    ns @2: UInt16;
    model @5: Model;
    enum Model {
        wikitext @0;
        wikibaseitem @1;
        css @2;
        javascript @3;
        scribunto @4;
    }
    union {
        redirect @3: Text;
        text @4: Text;
    }
}

enum EntityType { item @0; property @1; }

struct Entity {
    id @0: Text;
    type @1: EntityType;
    labels @2: Map(Text,LocalizedText);
    descriptions @3: Map(Text,LocalizedText);
    aliases @4: Map(Text,List(LocalizedText));
    claims @5: Map(Text,List(Claim));
    sitelinks @6: Map(Text,List(SiteLink));
}

struct Claim {
    id @0: Text;
    enum Type { statement @0; claim @1; }
    type @1: Type;
    mainsnak @2: Snak;
    enum Rank { prefered @0; normal @1; deprecated @2; }
    rank @3: Rank;
    qualifiers @4: Map(Text, Snak);
    references @5: List(Reference);
}

struct Snak {
    property @0: Text;
    union {
        value @1: DataValue;
        novalue @2: Void;
        somevalue @3: Void;
    }
    datatype @4: Text;
}

struct DataValue {
    union {
        string @0: Text;
        wikibaseentityid @1 : WikibaseEntityRef;
        globecoordinate @2: GlobeCoordinate;
    }
}

struct WikibaseEntityRef {
    type @0: EntityType;
    id @1: UInt32;
}

struct GlobeCoordinate {
    latitude @0: Float64;
    longitude @1: Float64;
    altitude @2: Float64;
    precision @3: Float64;
    globe @4: Text;
}

struct Reference {
    hash @0: Text;
    snaks @1: Map(Text, Snak);
    skaksorder @2: List(Text);
}

struct Time {
    time @0: Text;
    timezone @1: Int16;
    precision @2: UInt8;
    calendarmodel @3: Text;
}

struct Map(Key, Value) {
  entries @0 :List(Entry);
  struct Entry {
    key @0 :Key;
    value @1 :Value;
  }
}

struct LocalizedText {
    language @0: Text;
    value @1: Text;
    removed @2: Bool;
}

struct SiteLink {
    site @0: Text;
    title @1: Text;
    badges @2: List(Text);
}
