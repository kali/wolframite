@0x9ae0d1f47c29ffc8;

struct Page {
    id @0: UInt64;
    title @1: Text;
    ns @2: UInt8;
    union {
        redirect @3: Text;
        text @4: Text;
    }
}
