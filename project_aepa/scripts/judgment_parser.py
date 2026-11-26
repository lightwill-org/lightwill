"""
Split a Taiwanese criminal court document into its standard sections.

Documents follow a fixed template (headings on their own line):

    <court + document kind>            臺灣臺東地方法院刑事簡易判決
    <case number>                      112年度簡字第12號
    <party block>                      公訴人 / 被告 / 選任辯護人 ...
    <preamble>                         上列被告因...案件，...判決如下：
    主文
    事實 / 犯罪事實                     (facts)
    理由                               (reasons)
      or 事實及理由 / 犯罪事實及理由    (facts and reasons combined)
    中華民國 112 年 1 月 31 日          (closing date line; judges and clerk follow)
    附錄本案論罪科刑法條                (statutes appendix)
    附表 / 附件                         (tables; attachments such as the indictment,
                                        or the first-instance judgment in appeals)

Summary judgments often say "犯罪事實...引用如附件檢察官起訴書之記載", so the facts
live in the attachment. Facts found in the attachment are returned separately.

parse() returns character offsets into the original text (end exclusive), so sections
can be sliced without storing duplicate text. Defendant names are returned in plain
text for the caller to hash; they must never be written to disk unhashed.
"""

import re

WS_RE = re.compile(r"[\s　]+")
NUM = r"[\d０-９一二三四五六七八九十百零○〇]"

FACT_HEADS = {"事實", "犯罪事實", "犯罪事實及證據", "事實要旨", "犯罪事實要旨"}
REASON_HEADS = {"理由", "理由要旨"}
COMBINED_RE = re.compile(r"(?:犯罪)?事實(?:及|與|暨|、|並).{0,10}理由")
STATUTE_RE = re.compile(
    r"附錄.{0,16}法條.{0,6}|(?:本案)?所犯法條(?:全文)?|附錄法條.{0,4}|處罰條文|論罪科刑法條"
)
OTHER_HEADS = {"附記事項"}  # ends the previous section; not stored
ENUM_PREFIX_RE = re.compile(r"^[一二三四五六七八九十]{1,3}、")
_OPEN, _CLOSE = r"[【〔\[（(]?", r"[】〕\]）)]?"  # 【附件】 〔附表一〕
ATTACH_RE = re.compile(rf"{_OPEN}附件{NUM}*{_CLOSE}(?:[：:]|$)")
TABLE_RE = re.compile(rf"{_OPEN}附表{NUM}*{_CLOSE}(?:[：:]|$)")
# Some courts append the indictment right after the statutes without an 附件 heading
INDICTMENT_HEAD_RE = re.compile(
    r"(?:臺灣|台灣|福建).{0,16}檢察署.{0,12}(?:起訴書|處刑書|併辦意旨書)"
)
CLOSING_RE = re.compile(rf"中華民國{NUM}+年{NUM}+月{NUM}+日")
MAX_HEADING_LEN = 20

# Inside an attachment (indictment), the facts section ends at one of these
ATTACH_FACT_END_RE = re.compile(r"^(?:證據|所犯法條|理由|此致|二、證據|貳、證據)")

HEADER_COURT_RE = re.compile(
    r"^((?:臺灣|台灣|福建)[一-鿿]{0,6}?(?:地方法院|高等法院(?:[一-鿿]{1,4}?分院)?)|最高法院)"
)
CASE_NO_LINE_RE = re.compile(rf"{NUM}+年度.{{1,8}}字第")
PREAMBLE_RE = re.compile(r"^(?:上|右)(?:列|開|揭|述)|因.{0,40}(?:案件|聲請|上訴|抗告)")

# Party roles, longest first so that e.g. 選任辯護人 wins over 辯護人
ROLES = sorted(
    [
        "附帶民事訴訟原告",
        "附帶民事訴訟被告",
        "被付保護處分人",
        "被付懲戒人",
        "再審聲請人",
        "聲明異議人",
        "非常上訴人",
        "選任辯護人",
        "指定辯護人",
        "義務辯護人",
        "公設辯護人",
        "告訴代理人",
        "自訴代理人",
        "法定代理人",
        "受判決人",
        "受處分人",
        "被上訴人",
        "再抗告人",
        "輔佐人",
        "代理人",
        "辯護人",
        "公訴人",
        "自訴人",
        "上訴人",
        "抗告人",
        "聲請人",
        "相對人",
        "受刑人",
        "被告人",
        "告訴人",
        "參與人",
        "第三人",
        "被告",
        "原告",
        "少年",
    ],
    key=len,
    reverse=True,
)
ROLE_MODIFIER_RE = re.compile(rf"^(?:上{NUM}+人之?|共同|兼|即|之|同)")
APPELLANT_ROLES = {
    "上訴人",
    "被上訴人",
    "抗告人",
    "再抗告人",
    "聲請人",
    "再審聲請人",
    "聲明異議人",
}
DEFENDANT_ROLES = {
    "被告",
    "被告人",
    "受刑人",
    "受判決人",
    "受處分人",
    "少年",
    "附帶民事訴訟被告",
}

NAME_RE = re.compile(r"^(?:[一-鿿]{2,4}|[一-鿿]{1,6}[．‧·・][一-鿿]{1,8})$")
MASKED_NAME_RE = re.compile(r"^[一-鿿]{0,2}[○◯〇Ｏ０0O]{1,3}[一-鿿]{0,2}$")
NOT_NAME_START = tuple("住居籍設送身國現")
NOT_NAME_END = tuple("號樓室巷弄路街段鄰縣鄉鎮")  # wrapped address fragments
NAME_CUT_RE = re.compile(r"[（(，,]")

LAW_LINE_RE = re.compile(
    r"^(?:修正前|修正後)?((?:中華民國)?[一-鿿]{1,24}?(?:法|條例|通則))第"
)
# Final sentence of the reasons: "依刑事訴訟法第299條第1項前段，動物保護法第25條…，判決如主文"
FINAL_CLAUSE_END_RE = re.compile(r"(?:判決|裁定|處刑)如主文")
LAW_SEGMENT_RE = re.compile(
    r"^(?:[一二三四五六七八九十]+、|[（(][一二三四五六七八九十]+[)）])?"
    r"(?:據上論斷|應|爰|逕|並|及|暨|依|、)*(?:修正前|修正後)?([一-鿿]{1,24}?(?:法|條例|通則))第"
)
LAW_REFERENCE_PREFIXES = tuple("同本該前上此各原")

# Project-specific flag: the Animal Protection Act's criminal articles (25, 25-1, 26).
# Other articles are excluded on purpose: dog-bite negligence cases cite 第7條 (owner's duty)
# and prior-record or sentencing remarks mention the act without an offence.
# 野生動物保護法 (old name of the Wildlife Conservation Act) is excluded by the lookbehind.
# Crime articles changed across revisions: 30 (違反保護規定致動物死亡, before the 2015
# amendment), 25 / 25-1 / 26, 27-1 (posting abuse videos online). Judgments also write
# "動物保護法第6條…而犯同法第25條". Owner-duty articles (5, 6, 7, 20) alone do not count.
_CRIME_ARTICLE = r"(?:25|二十五|26|二十六|27條之1|二十七條之一|30|三十)條?"
ANIMAL_ACT_CRIME_RE = re.compile(
    rf"(?<!野生)動物保護法第{_CRIME_ARTICLE}"
    rf"|(?<!野生)動物保護法第[^。]{{1,40}}?同法第{_CRIME_ARTICLE}"
    r"|致動物死亡罪"
)

VERDICT_PATTERNS = {
    "guilty": re.compile(r"處(?:有期徒刑|拘役|罰金|無期徒刑|死刑)|科罰金|免刑"),
    "acquitted": re.compile(r"無罪"),
    "not_accepted": re.compile(r"不受理"),
    "exempted": re.compile(r"免訴"),
    "dismissed": re.compile(r"駁回"),
    "revoked": re.compile(r"撤銷"),
}

SECTIONS = [
    "header",
    "main",
    "facts",
    "reasons",
    "combined",
    "statutes",
    "attach",
    "attach_facts",
]


def norm(line: str) -> str:
    return WS_RE.sub("", line)


def heading_kind(n: str) -> str | None:
    """Classify a normalized line as a section heading, or None."""
    if ATTACH_RE.match(n) or INDICTMENT_HEAD_RE.fullmatch(n):
        return "attach"
    if TABLE_RE.match(n):
        return "table"
    # Sentencing records (宣示判決筆錄) number their headings: 一、主文： 二、犯罪事實要旨：
    n = ENUM_PREFIX_RE.sub("", n).rstrip("：:")
    if n in OTHER_HEADS:
        return "other"
    if not n or len(n) > MAX_HEADING_LEN:
        return None
    if n == "主文":
        return "main"
    if n in FACT_HEADS:
        return "facts"
    if n in REASON_HEADS:
        return "reasons"
    if COMBINED_RE.fullmatch(n):
        return "combined"
    if STATUTE_RE.fullmatch(n):
        return "statutes"
    return None


def peel_roles(n: str) -> tuple[list[str], str]:
    """
    Strip leading role labels and modifiers (共同, 即, 上一人之 ...); return (roles, remainder).
    Modifiers are only stripped when a role follows or nothing is left, so a name that
    happens to start with a modifier character is not cut.
    """
    roles = []
    rest = n
    while rest:
        m = ROLE_MODIFIER_RE.match(rest)
        after = rest[m.end() :] if m else rest
        role = next((r for r in ROLES if after.startswith(r)), None)
        if role:
            roles.append(role)
            rest = after[len(role) :]
        elif m and not after:
            rest = ""
        else:
            break
    return roles, rest


def name_after_roles(raw: str, role_len: int) -> str:
    """Find the name token in the raw line after role_len non-space characters."""
    tokens = WS_RE.split(raw.strip())
    consumed = 0
    for i, tok in enumerate(tokens):
        if consumed >= role_len:
            return tok
        if consumed + len(tok) > role_len:
            return tok[role_len - consumed :]
        consumed += len(tok)
    return ""


def is_defendant(roles: list[str]) -> bool:
    """
    被告 / 受刑人 etc. are defendants. In the Supreme Court and in rulings the defendant is
    often labelled only 上訴人 / 抗告人 / 聲請人; those count unless a victim or private
    prosecutor role is attached. Prosecutors are filtered out later by the name check.
    """
    if any(r in DEFENDANT_ROLES or r.endswith("被告") for r in roles):
        return True
    return bool(roles) and all(r in APPELLANT_ROLES for r in roles)


def parse_parties(lines: list[str]) -> list[tuple[str, str]]:
    """Returns (role, name) for defendants in the party block; masked names included."""
    people = []
    current: list[str] = []
    for raw in lines:
        n = norm(raw)
        if not n or CASE_NO_LINE_RE.search(n):
            continue
        roles, rest = peel_roles(n)
        if roles:
            current = roles  # otherwise a continuation line: another person, same role
        if not rest:
            continue
        token = name_after_roles(raw, len(n) - len(rest))
        token = NAME_CUT_RE.split(token)[0]
        if token.startswith(NOT_NAME_START) or token.endswith(NOT_NAME_END):
            continue
        if not (NAME_RE.match(token) or MASKED_NAME_RE.match(token)):
            continue
        if is_defendant(current):
            people.append(("/".join(current), token))
    return list(dict.fromkeys(people))


def extract_laws(statutes_text: str, final_text: str) -> list[str]:
    laws = []
    for line in statutes_text.splitlines():
        m = LAW_LINE_RE.match(norm(line))
        if m:
            laws.append(m.group(1))
    final = norm(final_text)
    ends = list(FINAL_CLAUSE_END_RE.finditer(final))
    if ends:
        # The clause is the sentence that ends with 判決如主文
        clause = final[: ends[-1].start()].rsplit("。", 1)[-1]
        for segment in re.split(r"[，,；;]", clause):
            m = LAW_SEGMENT_RE.match(segment)
            if m:
                laws.append(m.group(1))
    cleaned = []
    for law in laws:
        law = law.removeprefix("中華民國")
        if len(law) >= 2 and not law.startswith(LAW_REFERENCE_PREFIXES):
            cleaned.append(law)
    return list(dict.fromkeys(cleaned))


def doc_kind(title: str) -> str:
    if "簡易判決" in title:
        return "summary_judgment"
    if "判決" in title:
        return "judgment"
    if "裁定" in title:
        return "ruling"
    return "other"


def attach_type(text: str) -> str:
    head = norm(text[:300])
    if "處刑書" in head:
        return "summary_indictment"
    if "起訴書" in head:
        return "indictment"
    if "移送併辦" in head:
        return "merge_request"
    if "判決" in head:
        return "judgment"
    return "other"


def parse(text: str) -> dict:
    lines = text.split("\n")
    starts = [0]
    for line in lines[:-1]:
        starts.append(starts[-1] + len(line) + 1)
    total = len(text)

    def span(a: int, b: int) -> tuple[int, int]:
        # A heading on the last line gives an empty section at the end of the text
        start = starts[a] if a < len(lines) else total
        return start, (starts[b] if b < len(lines) else total)

    normed = [norm(line) for line in lines]
    kinds = [heading_kind(n) for n in normed]
    title = normed[0] if normed else ""
    court = HEADER_COURT_RE.match(title)

    result = {
        "status": "ok",
        "doc_kind": doc_kind(title[:40]),
        "court_name": court.group(1) if court else "",
        "facts_heading": "",
        "reasons_as_facts": False,
        "attach_type": "",
        "defendants": [],
        "laws": [],
        "cites_animal_protection_act": False,
        **{f"{s}_span": (-1, -1) for s in SECTIONS},
        **{f"main_{k}": False for k in VERDICT_PATTERNS},
    }

    main_idx = next((i for i, k in enumerate(kinds) if k == "main"), None)
    if main_idx is None:
        result["status"] = "no_main"
        return result

    # Header and parties
    result["header_span"] = span(0, main_idx)
    preamble = next(
        (i for i in range(1, main_idx) if PREAMBLE_RE.search(normed[i])), main_idx
    )
    result["defendants"] = parse_parties(lines[1:preamble])

    # Body ends at the closing date line, or at the first tail section
    closing = next(
        (i for i in range(main_idx + 1, len(lines)) if CLOSING_RE.fullmatch(normed[i])),
        None,
    )
    tail_start = next(
        (
            i
            for i in range(main_idx + 1, len(lines))
            if kinds[i] in ("statutes", "attach", "table")
        ),
        len(lines),
    )
    body_end = closing if closing is not None else tail_start

    # Statutes inside the body (sentencing records: 四、處罰條文) are kept; tail ones come later
    body_heads = [
        (i, k)
        for i, k in enumerate(kinds[:body_end])
        if i >= main_idx and k and k not in ("attach", "table")
    ]
    body_heads.append((body_end, "end"))

    found: dict[str, tuple[int, int]] = {}
    for (i, k), (j, _) in zip(body_heads, body_heads[1:]):
        if (
            k == "reasons"
            and "reasons" in found
            and "facts" not in found
            and "combined" not in found
        ):
            # Old documents sometimes label the facts section 理由 as well
            found["facts"] = found.pop("reasons")
            result["reasons_as_facts"] = True
        if k not in found:
            found[k] = (i, j)
            if k in ("facts", "combined"):
                result["facts_heading"] = ENUM_PREFIX_RE.sub("", normed[i]).rstrip(
                    "：:"
                )
    found.pop("other", None)
    for k, (i, j) in found.items():
        result[f"{k}_span"] = span(i + 1, j)

    main_text = text[slice(*result["main_span"])]
    for k, pattern in VERDICT_PATTERNS.items():
        result[f"main_{k}"] = bool(pattern.search(main_text))

    # Tail: statutes appendix, tables, attachments (everything after 附件 is attachment)
    tail_from = closing if closing is not None else tail_start
    attach_idx = next(
        (i for i in range(tail_from, len(lines)) if kinds[i] == "attach"), None
    )
    tail_end = attach_idx if attach_idx is not None else len(lines)
    statutes_idx = next(
        (i for i in range(tail_from, tail_end) if kinds[i] == "statutes"), None
    )
    if statutes_idx is not None and "statutes" not in found:
        stop = next(
            (
                i
                for i in range(statutes_idx + 1, tail_end)
                if kinds[i] in ("table", "attach")
            ),
            tail_end,
        )
        result["statutes_span"] = span(statutes_idx + 1, stop)

    if attach_idx is not None:
        result["attach_span"] = span(attach_idx, len(lines))
        result["attach_type"] = attach_type(text[starts[attach_idx] :])
        fact_idx = next(
            (
                i
                for i in range(attach_idx + 1, len(lines))
                if kinds[i] in ("facts", "combined")
            ),
            None,
        )
        if fact_idx is not None:
            stop = next(
                (
                    i
                    for i in range(fact_idx + 1, len(lines))
                    if ATTACH_FACT_END_RE.match(normed[i])
                    or kinds[i] in ("reasons", "statutes", "attach")
                ),
                len(lines),
            )
            result["attach_facts_span"] = span(fact_idx + 1, stop)

    final_section = found.get("reasons") or found.get("combined")
    final_text = (
        text[slice(*span(final_section[0] + 1, final_section[1]))][-1200:]
        if final_section
        else ""
    )
    statutes_text = text[slice(*result["statutes_span"])]
    result["laws"] = extract_laws(statutes_text, final_text)

    # Search normalized text: law names in the appendix can be split by line wraps
    # (動\n物保護法), and appeals often cite the offence only in the reasons
    sources = [
        text[slice(*result[f"{k}_span"])] for k in ("main", "reasons", "combined")
    ]
    result["cites_animal_protection_act"] = bool(
        ANIMAL_ACT_CRIME_RE.search(norm(statutes_text + "".join(sources)))
    )

    if (
        "facts" not in found
        and "combined" not in found
        and result["attach_facts_span"][0] < 0
    ):
        result["status"] = "no_facts"
    return result


def section(text: str, row, name: str) -> str:
    """Slice a section from the original text using a structured row (dict or Series)."""
    a, b = row[f"{name}_start"], row[f"{name}_end"]
    return text[a:b] if a >= 0 else ""
