/**
 * How a value becomes a cell, and what the Source tab says (#1677 O-S9).
 *
 * A VALUE IS DATA. Each awkward value below is one a real server can hold: the fixture ships a node
 * whose bytes are not valid UTF-8, and a plant server holds strings a reader pasted. None of them is
 * rendered as markup, and none of them throws, because a throw here would lose the row and tell the
 * reader the node is empty.
 */
import { describe, expect, test } from "bun:test";
import {
  accessLevelWords,
  keepsHistory,
  nodeSourceDocument,
  OPCUA_OBJECT_KINDS,
  valueCell,
} from "@/lib/db/providers/industrial/opcua/objects";

describe("the object kinds", () => {
  test("one kind, so a node opens in a Source tab rather than a generated read", () => {
    expect(OPCUA_OBJECT_KINDS).toEqual(["node"]);
  });
});

describe("a value becomes a cell", () => {
  test("scalars carry their own text", () => {
    expect(valueCell(42)).toEqual({ text: "42", shape: "number" });
    expect(valueCell(true)).toEqual({ text: "true", shape: "boolean" });
    expect(valueCell("ok")).toEqual({ text: "ok", shape: "string" });
  });

  /** An Int64 arrives as a bigint, which `JSON.stringify` throws on if it reaches it unguarded. */
  test("a bigint is text and not a throw", () => {
    expect(valueCell(BigInt("9007199254740993"))).toEqual({
      text: "9007199254740993",
      shape: "bigint",
    });
    expect(valueCell([BigInt(1), BigInt(2)]).text).toBe('["1","2"]');
  });

  test("a DateTime is an instant in ISO 8601", () => {
    expect(valueCell(new Date("2026-10-12T01:23:45.678Z")).text).toBe("2026-10-12T01:23:45.678Z");
  });

  /**
   * THE FIXTURE HOLDS BYTES THAT ARE NOT TEXT. Decoding them would substitute replacement
   * characters for the reader's own data, so a ByteString is hex.
   */
  test("a ByteString is hex, including bytes that are not valid UTF-8", () => {
    expect(valueCell(new Uint8Array([0xff, 0xfe, 0x00, 0x41]))).toEqual({
      text: "fffe0041",
      shape: "bytes",
    });
  });

  test("a byte array inside a structure is hex too, rather than a throw", () => {
    expect(valueCell({ b: new Uint8Array([0xde, 0xad]) }).text).toBe('{"b":"dead"}');
  });

  /** O-S9: markup and escapes are carried, never interpreted. */
  test("HTML and ANSI escapes are carried through unchanged", () => {
    expect(valueCell("<script>alert(1)</script>").text).toBe("<script>alert(1)</script>");
    expect(valueCell("\u001b[31mred\u001b[0m").text).toBe("\u001b[31mred\u001b[0m");
  });

  /** Measured on a staged server: a 100x100 Int32 matrix arrives with its dimensions beside it. */
  /**
   * THE LIBRARY ANSWERS A TYPED ARRAY FOR EVERY NUMERIC ARRAY, which `Array.isArray` reads as false:
   * measured, a 100x100 Int32 matrix arrives as an `Int32Array` and a Double array as a
   * `Float64Array`, and without this branch `JSON.stringify` wrote them as objects keyed by index
   * (`{"0":1,"1":2}`), which is a reader's data rendered as something else.
   */
  test("a typed array is an array, and a ByteString is still hex", () => {
    expect(valueCell(new Int32Array([1, 2, 3]))).toEqual({
      text: "[1,2,3]",
      shape: "array",
    });
    expect(valueCell(new Float64Array([1.5, 2.5]))).toEqual({
      text: "[1.5,2.5]",
      shape: "array",
    });
    expect(valueCell(new Uint8Array([0x01, 0x02]))).toEqual({
      text: "0102",
      shape: "bytes",
    });
  });

  test("an array and a matrix each arrive as an array", () => {
    expect(valueCell([1, 2, 3])).toEqual({ text: "[1,2,3]", shape: "array" });
    expect(
      valueCell([
        [1, 2],
        [3, 4],
      ]).shape,
    ).toBe("array");
  });

  test("a LocalizedText-shaped structure is JSON", () => {
    expect(valueCell({ locale: "tr", text: "Kazan" }).text).toBe('{"locale":"tr","text":"Kazan"}');
  });

  test("an absent value is empty rather than the word null", () => {
    expect(valueCell(null)).toEqual({ text: "", shape: "null" });
    expect(valueCell(undefined)).toEqual({ text: "", shape: "null" });
  });

  test("nothing a server can hold makes this throw", () => {
    for (const value of [0, -0, Number.NaN, Infinity, "", [], {}, new Uint8Array(), new Date(0), BigInt(0)]) {
      expect(() => valueCell(value)).not.toThrow();
    }
  });
});

describe("AccessLevel, decoded (Part 3 8.57)", () => {
  /** The values measured on the fixture: 3 on a writable node, 5 on the historized one. */
  test("the measured levels read as words", () => {
    expect(accessLevelWords(3)).toBe("read, write");
    expect(accessLevelWords(5)).toBe("read, history");
    expect(accessLevelWords(1)).toBe("read");
    expect(accessLevelWords(7)).toBe("read, write, history");
  });

  test("zero is none, and absent says so rather than reading as none", () => {
    expect(accessLevelWords(0)).toBe("none");
    expect(accessLevelWords(undefined)).toBe("not stated");
  });
});

describe("whether a node keeps history", () => {
  /**
   * READ FROM THE ATTRIBUTE, NOT ATTEMPTED AND RECOVERED FROM, because the three stacks answer a
   * history read of a non-historized node three different ways: BadHistoryOperationUnsupported per
   * node, BadNotSupported for the whole request, and Good with zero values.
   */
  test("Historizing true is enough", () => {
    expect(keepsHistory({ historizing: true })).toBe(true);
  });

  test("the HistoryRead bit is enough, for a server that sets it without Historizing", () => {
    expect(keepsHistory({ accessLevel: 5 })).toBe(true);
    expect(keepsHistory({ accessLevel: 7 })).toBe(true);
  });

  test("a writable node with no history is not historized", () => {
    expect(keepsHistory({ accessLevel: 3 })).toBe(false);
    expect(keepsHistory({})).toBe(false);
  });
});

describe("the Source document", () => {
  test("it carries the value, its status and both timestamps", () => {
    const text = nodeSourceDocument(
      "ns=3;s=HistorianInt32",
      "Variable",
      {
        nodeId: "ns=3;s=HistorianInt32",
        value: 34,
        statusCode: "Good",
        sourceTimestamp: "2026-10-12T01:00:00.000Z",
        serverTimestamp: "2026-10-12T01:00:00.001Z",
      },
      { accessLevel: 5, historizing: true, dataType: "i=6" },
    );
    const parsed = JSON.parse(text) as Record<string, unknown>;
    expect(parsed.value).toBe("34");
    expect(parsed.statusCode).toBe("Good");
    expect(parsed.historizing).toBe(true);
    expect(parsed.keepsHistory).toBe(true);
    expect(parsed.accessLevel).toBe("read, history");
  });

  /*
   * THE ATTRIBUTE AND THE DERIVED ANSWER ARE TWO KEYS, and the document used to carry one: the
   * derived value, under the attribute's own name. A server that says `Historizing` false while
   * setting the `HistoryRead` bit of AccessLevel - which is the case the derivation exists for - was
   * therefore reported as `historizing: true`, an attribute the server did not set. `docs/providers/
   * opcua.md` 6.2 forbids exactly that, and said so while the code did it.
   */
  test("a node whose server says Historizing false keeps that answer, beside the derived one", () => {
    const parsed = JSON.parse(
      nodeSourceDocument("ns=3;s=X", "Variable", undefined, {
        historizing: false,
        accessLevel: 5,
      }),
    ) as Record<string, unknown>;

    expect(parsed.historizing).toBe(false);
    expect(parsed.keepsHistory).toBe(true);
  });

  /*
   * `""` IS A VALUE AND `null` IS NOT, and the document used to print the first for the second: a
   * Read of a class with no Value attribute answers a DataValue carrying the Bad status alone, whose
   * cell text is empty, and the document showed `value: ""` beside `valueShape: "null"`. Measured
   * live on the fixture's `Locations` and `NetworkSet`.
   */
  test("a node that holds no value says null, while one holding the empty string says so", () => {
    const absent = JSON.parse(
      nodeSourceDocument(
        "ns=0;i=1",
        "Object",
        {
          nodeId: "ns=0;i=1",
          value: null,
          statusCode: "BadAttributeIdInvalid",
        },
        {},
      ),
    ) as Record<string, unknown>;
    expect(absent.value).toBeNull();
    expect(absent.valueShape).toBe("null");

    const empty = JSON.parse(
      nodeSourceDocument("ns=3;s=S", "Variable", { nodeId: "ns=3;s=S", value: "", statusCode: "Good" }, {}),
    ) as Record<string, unknown>;
    expect(empty.value).toBe("");
    expect(empty.valueShape).toBe("string");
  });

  test("a node whose server states no Historizing attribute says null rather than false", () => {
    const parsed = JSON.parse(nodeSourceDocument("ns=3;s=X", "Variable", undefined, { accessLevel: 1 })) as Record<
      string,
      unknown
    >;

    expect(parsed.historizing).toBeNull();
    expect(parsed.keepsHistory).toBe(false);
  });

  /**
   * A NODE WHOSE CLASS HAS NO VALUE SAYS SO, rather than showing a status the reader reads as a
   * failure. Measured on the documented fixture: the Keys panel's FIRST page holds `Locations` and
   * `NetworkSet`, both childless Objects, and clicking either answered a document whose statusCode
   * was `BadAttributeIdInvalid` and nothing else - an Object has no Value attribute at all
   * (Part 3 5.5.1), so a Read of one cannot succeed. The row stays openable, because drawn as a
   * folder it would open empty and drawn as neither it would vanish from the tree.
   */
  test("an Object's document says it carries no value, beside the status that proves it", () => {
    const parsed = JSON.parse(
      nodeSourceDocument(
        "ns=0;i=31915",
        "Object",
        {
          nodeId: "ns=0;i=31915",
          value: null,
          statusCode: "BadAttributeIdInvalid",
        },
        {},
      ),
    ) as Record<string, unknown>;
    expect(parsed.statusCode).toBe("BadAttributeIdInvalid");
    expect(String(parsed.note)).toBe(
      "Object has no Value attribute, so the BadAttributeIdInvalid below is expected, not a failed read.",
    );
    /*
     * TWO THINGS THE SENTENCE MUST NOT DO, both measured in a browser on the two nodes above. It must
     * not claim the node has CHILDREN: `Locations` and `NetworkSet` are drawn with no twisty and list
     * nothing under them. And it must FIT: the document is JSON in an editor that does not wrap, and
     * the first version ran off the right edge at about 110 characters of its 205, so the reader
     * never reached the half that explained the status.
     */
    expect(String(parsed.note)).not.toMatch(/children/);
    expect(String(parsed.note).length).toBeLessThanOrEqual(110);
  });

  /**
   * AND IT IS NOT ONLY OBJECTS. Part 3 gives a Value attribute to Variables alone, so a View, an
   * ObjectType and the rest answer the same way - a View node on the fixture opened with a bare
   * `BadAttributeIdInvalid` and no sentence, which is what this note exists to prevent. The class
   * names itself, so a reader learns which kind of node they clicked.
   */
  test.each(["View", "ObjectType", "DataType", "ReferenceType"])(
    "a %s node's document names its class and says it carries no value",
    (nodeClass) => {
      const parsed = JSON.parse(
        nodeSourceDocument("i=1", nodeClass, { nodeId: "i=1", value: null, statusCode: "BadAttributeIdInvalid" }, {}),
      ) as Record<string, unknown>;
      expect(String(parsed.note)).toBe(
        `${nodeClass} has no Value attribute, so the BadAttributeIdInvalid below is expected, not a failed read.`,
      );
      expect(String(parsed.note).length).toBeLessThanOrEqual(110);
    },
  );

  /*
   * A METHOD HAS NO VALUE ATTRIBUTE EITHER, and its note used to leave that out: the document said
   * what a Method is and then showed `BadAttributeIdInvalid` with nothing explaining it - the exact
   * defect the no-Value note exists to stop, left standing on the one class that was excluded from
   * it. Measured in a browser on `Server/GetMonitoredItems`, one row below an Object that did get
   * the explanation.
   */
  test("a Method's document says both why it is listed and why its status is Bad", () => {
    const method = JSON.parse(
      nodeSourceDocument(
        "ns=3;s=Reset",
        "Method",
        { nodeId: "ns=3;s=Reset", value: null, statusCode: "BadAttributeIdInvalid" },
        {},
      ),
    ) as Record<string, unknown>;
    expect(String(method.note)).toMatch(/never called by this provider/);
    expect(String(method.note)).toMatch(/BadAttributeIdInvalid is expected/);
    expect(String(method.note).length).toBeLessThanOrEqual(110);
    const variable = JSON.parse(
      nodeSourceDocument("ns=3;s=V", "Variable", { nodeId: "ns=3;s=V", value: 1, statusCode: "Good" }, {}),
    ) as Record<string, unknown>;
    expect(variable.note).toBeUndefined();
  });

  /** A Bad status is still a row: dropping it would say the node does not exist. */
  test("a node whose read failed is still a document, with the status in it", () => {
    const parsed = JSON.parse(
      nodeSourceDocument("i=2277", "Variable", { nodeId: "i=2277", value: null, statusCode: "BadNotReadable" }, {}),
    ) as Record<string, unknown>;
    expect(parsed.statusCode).toBe("BadNotReadable");
    // The status is the answer and there is no value: `null`, where this once said `""`.
    expect(parsed.value).toBeNull();
  });

  /** Measured: an admin saw UserAccessLevel 3 against AccessLevel 1, so both are shown. */
  test("AccessLevel and UserAccessLevel are both shown, because they differ in practice", () => {
    const parsed = JSON.parse(
      nodeSourceDocument("i=2994", "Variable", undefined, {
        accessLevel: 1,
        userAccessLevel: 3,
      }),
    ) as Record<string, unknown>;
    expect(parsed.accessLevel).toBe("read");
    expect(parsed.userAccessLevel).toBe("read, write");
  });

  /** A method is listed for orientation, and the row says it is never called. */
  test("a method's document says this provider never calls it", () => {
    const parsed = JSON.parse(nodeSourceDocument("ns=3;s=Reset", "Method", undefined, {})) as Record<string, unknown>;
    expect(String(parsed.note)).toMatch(/never called by this provider/);
  });
});
