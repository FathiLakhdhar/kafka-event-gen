const protobuf = require("protobufjs");
const protobufDescriptor = require("protobufjs/ext/descriptor");
const { faker, allFakers } = require("@faker-js/faker");
const { Kafka, Partitioners } = require("kafkajs");
const fs = require("fs");
const path = require("path");

const planPath = path.resolve(process.argv[2] || "plans/plan.json");
const planDir = path.dirname(planPath);
const plan = JSON.parse(fs.readFileSync(planPath, "utf8"));

// ---- merge defaults + events ----
const clean = (o) => Object.fromEntries(Object.entries(o).filter(([, v]) => v !== null));
const D = plan.defaults || {};
// the schema comes from a .proto source file or a compiled FileDescriptorSet, never both
const bothSchemas = (o) => o.proto != null && o.descriptor != null;
if (bothSchemas(D)) throw Error(`defaults: use either "proto" or "descriptor", not both`);
// a resend event sends again a message stored in a pool with "$message"; it doesn't use defaults,
// which would otherwise change the copy (a default topic or a fresh traceparent)
const RESEND_KEYS = ["resend", "type", "topic", "headers", "pools", "count", "weight"];
function resendEvent(e) {
  const r = e.resend;
  const label = e.type ?? `resend of ${JSON.stringify(typeof r === "string" ? r : r?.from)}`;
  if (!r || typeof r !== "object" || typeof r.from !== "string" || Object.keys(r).some((k) => k !== "from" && k !== "take"))
    throw Error(`${label}: "resend" needs { "from": "<pool>.<field>" } with an optional "take": true`);
  const extra = Object.keys(e).filter((k) => !RESEND_KEYS.includes(k));
  if (extra.length)
    throw Error(`${label}: a resend event sends the stored message unchanged, so it can't have ${extra.map((k) => `"${k}"`).join(", ")}`);
  return { type: label, topic: e.topic, count: e.count, weight: e.weight, resend: { from: r.from, take: !!r.take },
    rules: {}, headers: clean(e.headers || {}), pools: e.pools ?? {} };
}
const events = plan.events.map((e) => {
  if (e.resend != null) return resendEvent(e);
  if (bothSchemas(e)) throw Error(`${e.type}: use either "proto" or "descriptor", not both`);
  const m = { ...D, ...e, rules: clean({ ...D.rules, ...e.rules }), headers: clean({ ...D.headers, ...e.headers }) };
  if (e.proto != null) delete m.descriptor; // the event's own schema replaces the other kind from defaults
  if (e.descriptor != null) delete m.proto;
  if (e.payload) { // schemaless JSON event: the payload rules replace the proto schema
    if (e.proto || e.descriptor || e.rules)
      throw Error(`${e.type}: an event with "payload" can't also have "proto", "descriptor" or "rules"`);
    if (m.format && m.format !== "json") throw Error(`${e.type}: an event with "payload" is always sent as JSON`);
    m.payload = clean({ ...D.payload, ...e.payload });
    m.format = "json";
    m.rules = {};
    delete m.proto;
    delete m.descriptor;
  } else delete m.payload;
  m.format ??= "protobuf";
  if (m.pool) m.pools = { ...m.pools, [m.pool.name]: m.pool.add }; // old single-pool syntax
  delete m.pool;
  m.pools ??= {};
  return m;
});
events.forEach((e) => {
  if (e.resend) return;
  if (!e.type || !e.topic || !(e.proto || e.descriptor || e.payload))
    throw Error(`event needs type, topic, and a proto, a descriptor or a payload: ${JSON.stringify(e.type)}`);
  if (!["protobuf", "json"].includes(e.format)) throw Error(`${e.type}: "format" must be "protobuf" or "json"`);
});

// ---- weights or quotas ----
// weights: plan.count messages, event picked by weight
// quotas:  every event has an exact count, picked by how many it has left
const quotas = events.some((e) => e.count != null);
if (quotas) events.forEach((e) => {
  if (!Number.isInteger(e.count) || e.count < 0)
    throw Error(`${e.type}: when any event has "count", every event needs a whole-number "count"`);
  if (e.weight != null) throw Error(`${e.type}: use either "weight" or "count", not both`);
});
const total = quotas ? events.reduce((s, e) => s + e.count, 0) : plan.count;
if (quotas && plan.count != null && plan.count !== total)
  throw Error(`plan count ${plan.count} doesn't match the sum of event counts (${total}); remove it or fix it`);
if (!quotas && !(Number.isInteger(plan.count) && plan.count > 0))
  throw Error(`plan needs a positive "count" (or a "count" on every event)`);
const left = new Map(events.map((e) => [e, e.count]));

// ---- proto loading: .proto source files or binary FileDescriptorSets ----
const roots = {};
const types = {};
function loadRoot(src, isDescriptor) {
  const file = path.resolve(planDir, src);
  return (roots[file] ??= (() => {
    const what = isDescriptor ? "descriptor" : "proto";
    if (isDescriptor && !fs.existsSync(file)) throw Error(`${what} "${src}": no file at ${file}`);
    let set;
    try {
      if (isDescriptor) set = protobufDescriptor.FileDescriptorSet.decode(fs.readFileSync(file));
    } catch (err) {
      throw Error(`${what} "${src}": not a binary FileDescriptorSet (${err.message}); ` +
        `build it with protoc --include_imports --descriptor_set_out=<file>`);
    }
    try {
      return (isDescriptor ? protobuf.Root.fromDescriptor(set) : protobuf.loadSync(file)).resolveAll();
    } catch (err) {
      const hint = isDescriptor && /no such type/i.test(err.message)
        ? " (build the descriptor set with protoc --include_imports so imported types are in it)" : "";
      throw Error(`${what} "${src}": ${err.message}${hint}`);
    }
  })());
}
const typeOf = (e) => {
  const src = e.descriptor ?? e.proto;
  return (types[src + "|" + e.type] ??= (() => {
    const t = loadRoot(src, e.descriptor != null).lookup(e.type);
    if (!(t instanceof protobuf.Type))
      throw Error(`${e.type}: no message type with this name in ${e.descriptor != null ? "descriptor" : "proto"} "${src}"`);
    return t;
  })());
};

// ---- helpers ----
const get = (o, p) => p.split(".").reduce((a, k) => a?.[k], o);
const hex = (n) => faker.string.hexadecimal({ length: n, casing: "lower", prefix: "" });
const isTs = (t) => t && t.fullName === ".google.protobuf.Timestamp";
// a date, ISO string or epoch millis; a value already in Timestamp form (e.g. read from a pool) is kept
const toTs = (d) => {
  if (d && typeof d === "object" && !(d instanceof Date) && "seconds" in d) return { seconds: d.seconds, nanos: d.nanos ?? 0 };
  const ms = new Date(d).getTime();
  if (Number.isNaN(ms)) throw Error(`${JSON.stringify(d)} isn't a date, so it can't be a google.protobuf.Timestamp`);
  const seconds = Math.floor(ms / 1000);
  return { seconds, nanos: (ms - seconds * 1000) * 1e6 };
};
const fakerFn = (p, fk = faker) => {
  const i = p.lastIndexOf(".");
  const mod = i < 0 ? fk : get(fk, p.slice(0, i));
  const fn = mod?.[p.slice(i + 1)];
  return typeof fn === "function" ? fn.bind(mod) : null;
};

// ---- time ----
// "now" is the plan's fixed "clock" if it has one, otherwise the real time when each value is made
let clockMs = null;
if (plan.clock != null) {
  const ok = typeof plan.clock === "string" && /^\d{4}-\d{2}-\d{2}T.*(Z|[+-]\d{2}:\d{2})$/.test(plan.clock);
  clockMs = ok ? Date.parse(plan.clock) : NaN;
  if (Number.isNaN(clockMs))
    throw Error(`"clock" must be an ISO date-time with a time zone, like "2026-01-01T00:00:00Z"; got ${JSON.stringify(plan.clock)}`);
  for (const f of [faker, ...Object.values(allFakers)]) f.setDefaultRefDate(new Date(clockMs)); // past, future, recent
}
const nowMs = () => clockMs ?? Date.now();
const UNIT = { s: 1e3, m: 6e4, h: 36e5, d: 864e5, w: 6048e5 };
const TIME = /^(now|today)((?:[+-]\d+[smhdw])*)$/;
const isTime = (t) => typeof t === "string" && TIME.test(t);
// "now" or "today" (midnight UTC), plus any number of signed offsets: "now+7d", "today-1d+9h"
function timeOf(token) {
  const [, base, offsets] = token.match(TIME);
  let t = nowMs();
  if (base === "today") t = Math.floor(t / UNIT.d) * UNIT.d;
  for (const [, sign, n, u] of offsets.matchAll(/([+-])(\d+)([smhdw])/g)) t += (sign === "-" ? -1 : 1) * n * UNIT[u];
  return t;
}
const PRECISIONS = ["millis", "seconds", "date", "epochMillis", "epochSeconds"];
function isoValue(r) {
  let ms;
  if (Array.isArray(r.iso)) {
    const [from, to] = r.iso.map(timeOf);
    if (from > to) throw Error(`iso range ${JSON.stringify(r.iso)} ends before it starts`);
    ms = faker.date.between({ from, to }).getTime();
  } else if (["past", "future", "recent"].includes(r.iso)) ms = faker.date[r.iso]().getTime();
  else ms = timeOf(r.iso);
  const secs = Math.floor(ms / 1000);
  switch (r.precision ?? "millis") {
    case "seconds": return new Date(secs * 1000).toISOString().replace(".000Z", "Z");
    case "date": return new Date(ms).toISOString().slice(0, 10);
    case "epochMillis": return ms;
    case "epochSeconds": return secs;
    default: return new Date(ms).toISOString();
  }
}
function checkIso(r, where) {
  const v = r.iso;
  if (Array.isArray(v)) {
    if (v.length !== 2 || !v.every(isTime))
      throw Error(`${where}: an iso range needs two times like ["now", "now+7d"], got ${JSON.stringify(v)}`);
    if (timeOf(v[0]) > timeOf(v[1])) throw Error(`${where}: iso range ${JSON.stringify(v)} ends before it starts`);
  } else if (!["past", "future", "recent"].includes(v) && !isTime(v))
    throw Error(`${where}: unknown iso value ${JSON.stringify(v)} (expected "past", "future", "recent", ` +
      `"now" or "today" with optional offsets like "now+7d" or "today-1d+9h", or a range like ["now", "now+7d"])`);
  if (r.precision != null && !PRECISIONS.includes(r.precision))
    throw Error(`${where}: "precision" must be one of ${PRECISIONS.join(", ")}, got ${JSON.stringify(r.precision)}`);
}

// ---- collections: datasets (filled at startup) and pools (filled by events) ----
// a collection holds records in sub-pools; without "split" it has one unnamed sub-pool ""
const DS = plan.datasets || {};
const poolDefs = { ...plan.pools };
events.forEach((e) => Object.keys(e.pools).forEach((n) => {
  if (n in DS) throw Error(`${e.type}: can't add to "${n}", it's a dataset; datasets are only filled at startup`);
  poolDefs[n] ??= {};
}));

const store = {};
function makeCollection(kind, name, split) {
  if (name.includes(".") || name.includes("/")) throw Error(`${kind} "${name}": names can't contain "." or "/"`);
  if (split && (!Object.keys(split).length || Object.values(split).some((w) => !(w > 0))))
    throw Error(`${kind} "${name}": split needs one or more sub-pools with positive shares`);
  const subs = split ? Object.keys(split) : [""];
  return { kind, name, split, subs: Object.fromEntries(subs.map((s) => [s, []])), credit: Object.fromEntries(subs.map((s) => [s, 0])) };
}
for (const [n, d] of Object.entries(DS)) store[n] = makeCollection("dataset", n, d.split);
for (const [n, d] of Object.entries(poolDefs)) {
  if (n in store) throw Error(`"${n}" is both a dataset and a pool; names must be unique`);
  store[n] = makeCollection("pool", n, d.split);
}

// "name", "name/sub", "name.prop.path" or "name/sub.prop.path"
function parseSpec(spec) {
  const dot = spec.indexOf(".");
  const key = dot < 0 ? spec : spec.slice(0, dot);
  const [name, sub] = key.split("/");
  return { key, name, sub, prop: dot < 0 ? "" : spec.slice(dot + 1) };
}
const listsOf = (key) => {
  const { name, sub } = parseSpec(key);
  return sub != null ? [store[name].subs[sub]] : Object.values(store[name].subs);
};
const sizeOf = (key) => listsOf(key).reduce((s, l) => s + l.length, 0);

// split collections deal records out in a fixed rotation, so every sub-pool gets its exact share
function addRecord(c, rec) {
  let s = "";
  if (c.split) {
    const W = Object.values(c.split).reduce((a, b) => a + b, 0);
    for (const k in c.split) c.credit[k] += c.split[k];
    s = Object.keys(c.split).reduce((a, b) => (c.credit[b] > c.credit[a] ? b : a));
    c.credit[s] -= W;
  }
  const list = c.subs[s];
  list.push(rec);
  if (c.kind === "pool" && !quotas && list.length > 10000) list.shift(); // quotas rely on every record staying available
}

let picked = {};    // one record per collection (or sub-pool) per message
function pick(spec, take) {
  const { key, prop } = parseSpec(spec);
  if (!(key in picked)) {
    const lists = listsOf(key);
    const n = lists.reduce((s, l) => s + l.length, 0);
    if (!n) return undefined;
    let i = faker.number.int({ max: n - 1 });
    const list = lists.find((l) => (i < l.length ? true : ((i -= l.length), false)));
    picked[key] = take ? list.splice(i, 1)[0] : list[i];
  }
  return prop ? get(picked[key], prop) : picked[key];
}

// ---- rules ----
const RULES = ["static", "uuid", "iso", "regex", "oneOf", "int", "email", "faker",
  "field", "traceparent", "from", "fromPool", "ref"];
const READS = new Set(["from", "fromPool", "ref"]);
const specOf = (r) => r.from ?? r.fromPool ?? r.ref;

const scalar = (t) => {
  if (t === "string") return faker.lorem.words(2);
  if (t === "bool") return faker.datatype.boolean();
  if (t === "bytes") return Buffer.from(faker.string.alphanumeric(8));
  if (/float|double/.test(t)) return faker.number.float({ max: 1000, fractionDigits: 2 });
  return faker.number.int({ min: 0, max: 100000 });
};
const mapKey = (t) => (t === "string" ? faker.word.noun() : String(scalar(t)));

function byRule(r, obj) {
  if ("static" in r) return r.static;
  if (r.uuid) return faker.string.uuid();
  if ("iso" in r) return isoValue(r);
  if (r.regex) return faker.helpers.fromRegExp(r.regex);
  if (r.oneOf) return faker.helpers.arrayElement(r.oneOf);
  if (r.int) return faker.number.int({ min: r.int[0], max: r.int[1] });
  if (r.email) return fakerFor(r, obj).internet.email();
  if (r.faker) return fakerFn(r.faker, fakerFor(r, obj))(...(r.args || []));
  if (r.field) return get(obj, r.field);
  if (r.traceparent)
    return `00-${hex(32)}-${hex(16)}-${Math.random() < (r.traceparent.sampled ?? 1) ? "01" : "00"}`;
  if (specOf(r)) {
    const spec = specOf(r);
    let v = pick(spec, r.take);
    if (v == null && r.fallback) v = byRule(r.fallback, obj);
    if (v == null && !(parseSpec(spec).key in picked)) throw Error(`"${spec}" has no records left to read`);
    if (v == null && !r.optional) throw Error(`"${spec}": the record it read has no value there`);
    return v;
  }
  throw Error(`unknown rule ${JSON.stringify(r)}`);
}

// "locale" picks a localized faker: a locale id like "fr", or a rule that produces one
function fakerFor(r, obj) {
  if (!r.locale) return faker;
  const loc = typeof r.locale === "string" ? r.locale : byRule(r.locale, obj);
  if (!allFakers[loc]) throw Error(`unknown faker locale ${JSON.stringify(loc)}`);
  return allFakers[loc];
}
// a rule and the rules nested in it (fallback, locale)
const rulesIn = (r) => [r, ...(r.fallback ? rulesIn(r.fallback) : []),
  ...(r.locale && typeof r.locale === "object" ? rulesIn(r.locale) : [])];

// "unique": true regenerates until the value hasn't been used in this scope (a dataset or event field)
const seen = new Map();
function byRuleIn(scope, r, obj) {
  try {
    return uniqueByRule(scope, r, obj);
  } catch (err) {
    throw Error(`${scope}: ${err.message}`);
  }
}
function uniqueByRule(scope, r, obj) {
  if (!r.unique) return byRule(r, obj);
  const used = seen.get(scope) ?? seen.set(scope, new Set()).get(scope);
  for (let i = 0; i < 1000; i++) {
    const v = byRule(r, obj);
    if (v == null) return v; // an optional header left out; nothing to keep unique
    const k = typeof v === "object" ? JSON.stringify(v) : v;
    if (!used.has(k)) return used.add(k), v;
  }
  throw Error(`couldn't find an unused value after 1000 tries (${used.size} used); ` +
    `the rule allows too few different values for "unique"`);
}

// ---- validation (fail fast) ----
function checkSpec(spec, where, take) {
  if (typeof spec !== "string" || !spec) throw Error(`${where}: expected a dataset or pool name, got ${JSON.stringify(spec)}`);
  const { name, sub } = parseSpec(spec);
  const c = store[name];
  if (!c) throw Error(`${where}: no dataset or pool named "${name}"`);
  const subs = c.split ? Object.keys(c.split).join(", ") : "";
  if (sub != null && !(c.split && sub in c.split))
    throw Error(`${where}: ${c.kind} "${name}" has no sub-pool "${sub}"${subs ? ` (it has ${subs})` : ""}`);
  if (take && sub == null && c.split)
    throw Error(`${where}: can't take from all of "${name}"; take from one of its sub-pools (${subs})`);
  return c;
}
// returns the rule kind; collects the keys the rule reads without a fallback into `reads`
function checkRule(r, where, reads, inDataset = false, inHeader = false) {
  if (!r || typeof r !== "object") throw Error(`${where}: rule must be an object, got ${JSON.stringify(r)}`);
  const kind = RULES.find((k) => k in r);
  if (!kind) throw Error(`${where}: unknown rule ${JSON.stringify(r)} (expected one of ${RULES.join(", ")})`);
  if (r.optional != null) {
    if (typeof r.optional !== "boolean") throw Error(`${where}: "optional" must be true or false`);
    if (!inHeader) throw Error(`${where}: "optional" only works on a header's rule (not on fields, or inside a fallback or locale)`);
  }
  if (kind === "iso") checkIso(r, where);
  else if (r.precision != null) throw Error(`${where}: "precision" only works with iso rules`);
  if (kind === "traceparent" && typeof r.traceparent === "object") {
    const { sampled, ...rest } = r.traceparent;
    if (Object.keys(rest).length)
      throw Error(`${where}: traceparent only takes "sampled", not ${Object.keys(rest).map((k) => `"${k}"`).join(", ")}`);
    if (sampled != null && !(typeof sampled === "number" && sampled >= 0 && sampled <= 1))
      throw Error(`${where}: traceparent "sampled" must be a chance between 0 and 1, got ${JSON.stringify(sampled)}`);
  }
  if (kind === "faker" && !fakerFn(r.faker)) throw Error(`${where}: faker has no function "${r.faker}"`);
  if (r.locale != null) {
    if (kind !== "faker" && kind !== "email") throw Error(`${where}: "locale" only works with faker and email rules`);
    if (typeof r.locale === "string" && !allFakers[r.locale])
      throw Error(`${where}: unknown faker locale "${r.locale}" (e.g. ${["fr", "de", "es", "en_GB"].join(", ")})`);
    if (typeof r.locale === "object") checkRule(r.locale, `${where} locale`, reads, inDataset);
  }
  if (READS.has(kind)) {
    checkSpec(specOf(r), where, r.take && !inDataset); // dataset sizes are counted after building
    if (reads && !r.fallback) reads.push({ key: parseSpec(specOf(r)).key, take: !!r.take });
  }
  if (r.fallback) checkRule(r.fallback, `${where} fallback`, null, inDataset);
  return kind;
}
function checkPath(type, p, where) {
  let t = type;
  for (const name of p.split(".")) {
    const f = t instanceof protobuf.Type && t.fields[name];
    if (!f) throw Error(`${where}: no field "${p}" in ${type.fullName.slice(1)}`);
    t = f.resolvedType;
  }
  return t;
}

// ---- datasets: check, order by dependency, build ----
const dsDeps = {};
for (const [n, d] of Object.entries(DS)) {
  const where = `dataset "${n}"`;
  if (d.records != null) {
    if (!Array.isArray(d.records) || d.records.some((x) => !x || typeof x !== "object" || Array.isArray(x)))
      throw Error(`${where}: "records" must be a list of objects`);
    if (d.size != null || d.from != null) throw Error(`${where}: use one of "records", "size" or "from"`);
  } else if (d.from == null && !(Number.isInteger(d.size) && d.size >= 0))
    throw Error(`${where}: needs a whole-number "size", "from" another dataset, or a list of "records"`);
  if (d.size != null && !(Number.isInteger(d.size) && d.size >= 0)) throw Error(`${where}: "size" must be a whole number`);
  const deps = new Set();
  const dep = (spec, w) => {
    const c = checkSpec(spec, w);
    if (c.kind !== "dataset") throw Error(`${w}: datasets can only read other datasets, "${c.name}" is a pool`);
    deps.add(c.name);
  };
  if (d.from != null) {
    dep(d.from, `${where} from`);
    if (parseSpec(d.from).prop) throw Error(`${where}: "from" takes a dataset or sub-pool, not a field ("${d.from}")`);
  }
  for (const [k, r] of Object.entries(d.fields || {})) {
    const w = `${where} fields.${k}`;
    checkRule(r, w, null, true);
    for (const x of rulesIn(r)) if (specOf(x)) dep(specOf(x), w);
  }
  dsDeps[n] = deps;
}
const dsOrder = [];
const state = {};
const visit = (n, trail) => {
  if (state[n] === "done") return;
  if (state[n] === "busy") throw Error(`datasets depend on each other in a loop: ${[...trail, n].join(" -> ")}`);
  state[n] = "busy";
  dsDeps[n].forEach((m) => visit(m, [...trail, n]));
  state[n] = "done";
  dsOrder.push(n);
};
Object.keys(DS).forEach((n) => visit(n, []));

for (const n of dsOrder) {
  const d = DS[n];
  let base;
  if (d.from != null) {
    const all = listsOf(parseSpec(d.from).key).flat();
    const size = d.size ?? all.length;
    if (size > all.length)
      throw Error(`dataset "${n}": wants ${size} records from "${d.from}", which only has ${all.length}`);
    base = faker.helpers.arrayElements(all, size);
  } else if (d.records) base = d.records;
  else base = Array.from({ length: d.size }, () => ({}));
  for (const b of base) {
    picked = {};
    const rec = { ...b };
    for (const [k, r] of Object.entries(d.fields || {})) rec[k] = byRuleIn(`dataset "${n}" fields.${k}`, r, rec);
    addRecord(store[n], rec);
  }
  const c = store[n];
  const parts = c.split ? ` (${Object.entries(c.subs).map(([s, l]) => `${s} ${l.length}`).join(", ")})` : "";
  console.error(`dataset ${n}: ${sizeOf(n)} records${parts}`);
}
picked = {};

// ---- events: check against their proto or payload ----
// needs: keys an event reads without a fallback; the event is only eligible once they have records
const needs = new Map();
const takes = new Map(); // subset of needs the event removes a record from
events.forEach((e) => {
  const reads = [];
  let checkField;
  if (e.resend) {
    const where = e.type.startsWith("resend of ") ? e.type : `${e.type} resend`;
    const c = checkSpec(e.resend.from, where, e.resend.take);
    if (c.kind !== "pool") throw Error(`${where}: "${c.name}" is a dataset; messages to resend are kept in pools`);
    if (!parseSpec(e.resend.from).prop) throw Error(`${where}: name the record field that holds the message, e.g. "${c.name}.message"`);
    reads.push({ key: parseSpec(e.resend.from).key, take: e.resend.take });
    checkField = (p, where) => {
      throw Error(`${where}: a resend event has no fields of its own; use "$message", "$header.<name>" or "$from.<pool>"`);
    };
  } else if (e.payload) {
    const paths = Object.keys(e.payload);
    for (const [p, r] of Object.entries(e.payload)) {
      const clash = paths.find((q) => q.startsWith(p + "."));
      if (clash) throw Error(`${e.type} payload: sets both "${p}" and "${clash}"`);
      checkRule(r, `${e.type} payload.${p}`, reads);
    }
    // a path can name a payload value, an object above it, or a property inside a linked record
    checkField = (p, where) => {
      if (!paths.some((q) => q === p || q.startsWith(p + ".") || p.startsWith(q + ".")))
        throw Error(`${where}: no field "${p}" in the payload of ${e.type}`);
    };
  } else {
    const Type = typeOf(e);
    checkField = (p, where) => checkPath(Type, p, where);
    for (const [p, r] of Object.entries(e.rules)) {
      const t = checkPath(Type, p, `${e.type} rules`);
      if (isTs(t) && /^epoch/.test(r?.precision))
        throw Error(`${e.type} rules.${p}: "precision": "${r.precision}" makes a number, which doesn't fit a google.protobuf.Timestamp`);
      if (checkRule(r, `${e.type} rules.${p}`, reads) === "field")
        throw Error(`${e.type} rules.${p}: "field" rules only work in headers, payloads and datasets`);
    }
  }
  for (const [h, r] of Object.entries(e.headers))
    if (checkRule(r, `${e.type} headers.${h}`, reads, false, true) === "field" && e.resend)
      throw Error(`${e.type} headers.${h}: "field" rules don't work on resend events`);
  if (e.key) checkField(e.key, `${e.type} key`);
  for (const [pn, add] of Object.entries(e.pools))
    for (const [k, src] of Object.entries(add)) {
      const where = `${e.type} pools.${pn}.${k}`;
      if (src === "$message" || src.startsWith("$header.")) continue;
      if (src.startsWith("$from.")) reads.push({ key: parseSpec(src.slice(6)).key, take: false }), checkSpec(src.slice(6), where);
      else checkField(src, where);
    }
  needs.set(e, new Set(reads.map((x) => x.key)));
  takes.set(e, new Set(reads.filter((x) => x.take).map((x) => x.key)));
});

// a resend reads a record field that some event fills with "$message"
for (const e of events) {
  if (!e.resend) continue;
  const { name, prop } = parseSpec(e.resend.from);
  if (!events.some((x) => x.pools[name]?.[prop] === "$message"))
    throw Error(`${e.type.startsWith("resend of ") ? e.type : `${e.type} resend`}: no event stores a message in "${name}.${prop}"; add "${prop}": "$message" to an event's pools.${name}`);
}

// every pool that is read needs an event that adds to it
for (const c of Object.values(store)) {
  if (c.kind !== "pool") continue;
  const readers = events.filter((e) => [...needs.get(e)].some((k) => parseSpec(k).name === c.name));
  if (readers.length && !events.some((e) => c.name in e.pools))
    throw Error(`pool "${c.name}": ${readers.map((e) => e.type).join(", ")} read from it, but no event adds to it`);
}

// with quotas, make sure every dataset or pool has enough records for the events that read it
if (quotas) {
  const list = (es) => es.map((e) => `${e.type} (${e.count})`).join(" + ");
  const sum = (es) => es.reduce((s, e) => s + e.count, 0);
  const keys = new Set(events.flatMap((e) => [...needs.get(e)]));
  for (const key of keys) {
    const { name, sub } = parseSpec(key);
    const c = store[name];
    let supply, from;
    if (c.kind === "dataset") {
      supply = sizeOf(key);
      from = `dataset "${key}" only has ${supply}`;
    } else {
      const adders = events.filter((e) => name in e.pools && e.count > 0);
      const added = sum(adders);
      const W = c.split ? Object.values(c.split).reduce((a, b) => a + b, 0) : 1;
      supply = sub != null ? Math.floor(added * c.split[sub] / W) : added;
      from = sub != null ? `${list(adders)} add ${added}, and only ${supply} of them go to "${key}"`
        : `${list(adders)} only add ${added}`;
    }
    // reading all of a split collection competes with every take from its sub-pools
    const takesKey = (e) => takes.get(e).has(key) || (sub == null && [...takes.get(e)].some((k) => parseSpec(k).name === name));
    const takers = events.filter((e) => takesKey(e) && e.count > 0);
    const readers = events.filter((e) => needs.get(e).has(key) && !takes.get(e).has(key) && e.count > 0);
    const taken = sum(takers);
    if (!supply && (takers.length || readers.length))
      throw Error(`"${key}": ${list([...takers, ...readers])} read from it, but it never gets any records`);
    if (taken > supply) throw Error(`"${key}": ${list(takers)} take ${taken} records, but ${from}`);
    if (readers.length && taken === supply)
      throw Error(`"${key}": ${list(takers)} take all ${supply} records, so ${list(readers)} may find it empty; ` +
        `add more records or take fewer`);
  }
}

// ---- generation ----
function value(f, rules, depth, p) {
  const t = f.resolvedType;
  if (rules[p]) {
    const scope = `${f.parent.fullName.slice(1)}.${f.name}`;
    const v = byRuleIn(scope, rules[p]);
    try {
      return isTs(t) ? toTs(v) : v;
    } catch (err) {
      throw Error(`${scope}: ${err.message}`);
    }
  }
  if (isTs(t)) return toTs(faker.date.recent());
  if (t instanceof protobuf.Enum) {
    const names = Object.keys(t.values);
    const real = names.filter((n) => !n.endsWith("_UNSPECIFIED"));
    return faker.helpers.arrayElement(real.length ? real : names);
  }
  if (t instanceof protobuf.Type) return gen(t, rules, depth + 1, p);
  return scalar(f.type);
}

function gen(type, rules, depth = 0, prefix = "") {
  const o = {};
  const pathOf = (n) => (prefix ? `${prefix}.${n}` : n);
  const ruled = (p) => Object.keys(rules).some((k) => k === p || k.startsWith(p + "."));

  const skip = new Set();
  for (const oo of type.oneofsArray) {
    const names = oo.fieldsArray.map((f) => f.name);
    const synthetic = oo.name.startsWith("_") && names.length === 1;
    const forced = names.find((n) => ruled(pathOf(n)));
    const pickName = forced ?? (synthetic && Math.random() < 0.3 ? null : faker.helpers.arrayElement(names));
    names.forEach((n) => n !== pickName && skip.add(n));
  }

  for (const f of type.fieldsArray) {
    if (skip.has(f.name)) continue;
    const p = pathOf(f.name);
    if (f.resolvedType instanceof protobuf.Type && !isTs(f.resolvedType) && depth > 3) continue;
    if (f.map) o[f.name] = { [mapKey(f.keyType)]: value(f, rules, depth, p) };
    else if (f.repeated)
      o[f.name] = Array.from({ length: faker.number.int({ min: 1, max: 3 }) }, () => value(f, rules, depth, p));
    else o[f.name] = value(f, rules, depth, p);
  }
  return o;
}

// schemaless payload: dotted paths build nested objects; "field" rules see the values set so far
function buildPayload(e) {
  const o = {};
  for (const [p, r] of Object.entries(e.payload)) {
    const keys = p.split(".");
    let t = o;
    for (const k of keys.slice(0, -1)) t = t[k] ??= {};
    t[keys.at(-1)] = byRuleIn(`${e.type}.${p}`, r, o);
  }
  return o;
}

// proto3 JSON mapping: enum names, int64 as strings, bytes as base64, Timestamp as RFC 3339
const VIEW = { enums: String, longs: String, bytes: String };
function protoJson(type, obj) {
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    const f = type.fields[k];
    const one = (x) => isTs(f.resolvedType)
      ? new Date(Number(x.seconds) * 1000 + Math.round((x.nanos || 0) / 1e6)).toISOString()
      : f.resolvedType instanceof protobuf.Type ? protoJson(f.resolvedType, x) : x;
    out[k] = f.map ? Object.fromEntries(Object.entries(v).map(([mk, mv]) => [mk, one(mv)]))
      : Array.isArray(v) ? v.map(one) : one(v);
  }
  return out;
}

function makeMessage(i) {
  const eligible = events.filter((e) => (!quotas || left.get(e) > 0) && [...needs.get(e)].every((k) => sizeOf(k) > 0));
  if (!eligible.length) throw Error("no event can run: every remaining event needs a record from an empty dataset or pool");
  const e = faker.helpers.weightedArrayElement(
    eligible.map((x) => ({ weight: quotas ? left.get(x) : x.weight ?? 1, value: x })));
  if (quotas) left.set(e, left.get(e) - 1);
  picked = {};

  // obj: values the rules see; shown: what the dry run prints; bytes: what goes on the wire
  let obj, shown, bytes, type = e.type, format = e.format, topic = e.topic, key;
  const headers = {};
  if (e.resend) {
    const m = pick(e.resend.from, e.resend.take);
    if (!m || !Buffer.isBuffer(m.value)) throw Error(`${e.type}: "${e.resend.from}" doesn't hold a stored message`);
    ({ obj, shown, value: bytes, type, format, key } = m);
    topic ??= m.topic;
    Object.assign(headers, m.headers);
  } else if (e.payload) shown = obj = buildPayload(e);
  else {
    const Type = typeOf(e);
    obj = gen(Type, e.rules);
    const msg = Type.fromObject(obj); // converts enum names, map keys, nested objects
    const err = Type.verify(msg);
    if (err) throw Error(`${e.type}: ${err}`);
    if (e.format === "json") shown = protoJson(Type, Type.toObject(msg, VIEW));
    else {
      bytes = Buffer.from(Type.encode(msg).finish());
      shown = Type.toObject(Type.decode(bytes), VIEW);
    }
  }
  if (!e.resend) {
    if (e.format === "json") bytes = Buffer.from(JSON.stringify(shown));
    if (plan.typeHeader) headers[plan.typeHeader] = e.type;
    const k = e.key ? get(obj, e.key) : undefined;
    key = k != null ? String(k) : null;
  }
  for (const [h, r] of Object.entries(e.headers)) {
    const v = byRuleIn(`${e.type} headers.${h}`, r, obj); // applies "unique" like other rules
    if (v == null) {
      if (r.optional) continue; // left out (on a resend, the copied header stays)
      throw Error(`${e.type} headers.${h}: rule produced no value; add "optional": true to leave the header out instead`);
    }
    headers[h] = String(v);
  }

  for (const [pn, add] of Object.entries(e.pools)) {
    const rec = {};
    for (const [k, src] of Object.entries(add))
      rec[k] = src === "$message" ? { topic, key, value: bytes, headers: { ...headers }, shown, obj, type, format }
        : src.startsWith("$header.") ? headers[src.slice(8)]
        : src.startsWith("$from.") ? pick(src.slice(6), false)
        : get(obj, src);
    addRecord(store[pn], rec);
  }

  if (plan.outDir) {
    const dir = path.resolve(planDir, plan.outDir);
    fs.mkdirSync(dir, { recursive: true });
    const short = type.split(".").pop();
    const ext = format === "json" ? "json" : "protobin";
    fs.writeFileSync(path.join(dir, `${String(i + 1).padStart(4, "0")}-${short}.${ext}`), bytes);
  }

  return { topic, shown, msg: { key, value: bytes, headers } };
}

// ---- Kafka connection: brokers, TLS and SASL ----
// "${ENV:NAME}" in any of their strings reads an environment variable, so secrets stay out of the plan.
// Values read for ssl and sasl, and every secret field, are replaced by *** in anything we print.
const ENV_REF = /\$\{ENV:([A-Za-z_][A-Za-z0-9_]*)\}/g;
const SECRET_KEYS = new Set(["password", "key", "passphrase", "secretAccessKey", "sessionToken", "token"]);
const SASL_NEEDS = {
  plain: ["username", "password"],
  "scram-sha-256": ["username", "password"],
  "scram-sha-512": ["username", "password"],
  aws: ["authorizationIdentity", "accessKeyId", "secretAccessKey"],
  oauthbearer: ["token"],
};
const secrets = new Set();
// longest first, so a short value can't break up a longer one before it's replaced
const redact = (text) => [...secrets].sort((a, b) => b.length - a.length).reduce((t, v) => t.split(v).join("***"), String(text));

// resolves ${ENV:...} in every string of a value. In ssl and sasl (`hide`), secret fields and values
// read from the environment are kept for redaction.
function withEnv(v, where, hide, secret = false) {
  if (typeof v === "string") {
    const out = v.replace(ENV_REF, (_, name) => {
      const val = process.env[name];
      if (val == null || val === "") throw Error(`${where}: environment variable ${name} is ${val == null ? "not set" : "empty"}`);
      return val;
    });
    if (/\$\{\s*env\s*:/i.test(v.replace(ENV_REF, "")))
      throw Error(`${where}: write environment variables as \${ENV:NAME}, with NAME in letters, digits and _`);
    if (hide && out && (secret || out !== v)) secrets.add(out);
    return out;
  }
  if (Array.isArray(v)) return v.map((x, i) => withEnv(x, `${where}[${i}]`, hide, secret));
  if (v && typeof v === "object")
    return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, withEnv(x, `${where}.${k}`, hide, secret || SECRET_KEYS.has(k))]));
  return v;
}

// checked at startup, also in a dry run (which doesn't read the environment, since it doesn't connect)
function checkConnection() {
  const { ssl, sasl } = plan;
  if (ssl != null && typeof ssl !== "boolean" && (typeof ssl !== "object" || Array.isArray(ssl)))
    throw Error(`"ssl" must be true, false or an object of TLS options like { "ca": ..., "rejectUnauthorized": true }`);
  if (sasl != null) {
    if (typeof sasl !== "object" || Array.isArray(sasl)) throw Error(`"sasl" must be an object like { "mechanism": "scram-sha-512", ... }`);
    const mech = String(sasl.mechanism ?? "").toLowerCase();
    if (!SASL_NEEDS[mech])
      throw Error(`sasl.mechanism must be one of ${Object.keys(SASL_NEEDS).join(", ")}, got ${JSON.stringify(sasl.mechanism)}`);
    for (const k of SASL_NEEDS[mech])
      if (typeof sasl[k] !== "string" || !sasl[k]) throw Error(`sasl: "${mech}" needs "${k}" (e.g. "\${ENV:KAFKA_${k.replace(/[A-Z]/g, (c) => "_" + c).toUpperCase()}}")`);
  }
  // a secret written straight into the plan works, but it ends up in version control
  const literal = (o, where) => Object.entries(o || {}).forEach(([k, v]) => {
    if (SECRET_KEYS.has(k) && typeof v === "string" && !v.includes("${"))
      console.error(`warning: ${where}.${k} is written in the plan; read it from the environment with "\${ENV:NAME}"`);
  });
  literal(sasl, "sasl");
  if (typeof ssl === "object") literal(ssl, "ssl");
}
checkConnection();

function kafkaConfig() {
  const brokers = withEnv(plan.brokers, "brokers", false);
  if (!Array.isArray(brokers) || !brokers.length) throw Error(`"brokers" must be a list like ["localhost:9092"]`);
  const config = {
    clientId: withEnv(plan.clientId || "kafka-proto-gen", "clientId", false),
    brokers: brokers.flatMap((b) => String(b).split(",")).map((b) => b.trim()).filter(Boolean), // one variable can hold "host1:9093,host2:9093"
    logCreator: () => ({ namespace, label, log }) => {
      const { message, timestamp, logger, stack, ...extra } = log;
      const rest = Object.keys(extra).length ? " " + JSON.stringify(extra) : "";
      console.error(redact(`kafkajs ${label} ${namespace}: ${message}${rest}`));
    },
  };
  if (plan.ssl != null) {
    config.ssl = typeof plan.ssl === "object" ? withEnv(plan.ssl, "ssl", true) : plan.ssl;
    for (const k of ["ca", "cert", "key"])
      for (const pem of [config.ssl[k]].flat().filter((x) => typeof x === "string"))
        if (!pem.includes("-----BEGIN"))
          throw Error(`ssl.${k}: expected PEM text (starting with -----BEGIN), not a file path; put the file's contents in an environment variable and use "\${ENV:NAME}"`);
  }
  if (plan.sasl != null) {
    const sasl = withEnv(plan.sasl, "sasl", true);
    sasl.mechanism = sasl.mechanism.toLowerCase();
    if (sasl.mechanism === "oauthbearer") {
      const value = sasl.token;
      delete sasl.token;
      sasl.oauthBearerProvider = async () => ({ value }); // kafkajs wants a function; the plan gives a token
    }
    config.sasl = sasl;
  }
  return config;
}

// ---- run ----
let stopping = false;
process.on("SIGINT", () => {
  if (stopping) process.exit(130);
  stopping = true;
  console.log("stopping after current batch (Ctrl+C again to force)");
});

(async () => {
  let producer;
  try {
    if (!plan.dryRun) {
      producer = new Kafka(kafkaConfig()).producer({ createPartitioner: Partitioners.DefaultPartitioner });
      await producer.connect();
    }
    const rate = plan.ratePerSec || total;
    let sent = 0;
    while (sent < total && !stopping) {
      const t0 = Date.now();
      const n = Math.min(rate, total - sent);
      const batch = Array.from({ length: n }, (_, j) => makeMessage(sent + j));

      if (plan.dryRun) {
        batch.forEach((b) => console.log(JSON.stringify({
          topic: b.topic, key: b.msg.key, headers: b.msg.headers, value: b.shown,
        })));
      } else {
        const byTopic = {};
        batch.forEach((b) => (byTopic[b.topic] ??= []).push(b.msg));
        await producer.sendBatch({
          topicMessages: Object.entries(byTopic).map(([topic, messages]) => ({ topic, messages })),
        });
      }

      sent += n;
      console.error(`sent ${sent}/${total}`);
      const wait = 1000 - (Date.now() - t0);
      if (!plan.dryRun && sent < total && !stopping && wait > 0) await new Promise((r) => setTimeout(r, wait));
    }
  } catch (err) {
    console.error(redact(err.message || err));
    process.exitCode = 1;
  } finally {
    if (producer) await producer.disconnect().catch(() => {});
  }
})();
