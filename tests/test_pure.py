"""Standalone unit tests for Failovarr's PURE (string-only) functions.

These lock the deterministic matching contract — the normalize/key/classify logic
that decides how channels consolidate. They need NO Dispatcharr/Django runtime:
plugin.py's only module-load imports outside the stdlib are `django.db` and the
`apps.*` models, and none of the pure functions touch them, so we stub those three
and import plugin.py by path.

Run:
    python tests/test_pure.py     # plain, no dependencies
    pytest tests/                 # if pytest is installed
"""
import importlib.util
import os
import sys
import types

# --- stub the Dispatcharr/Django imports plugin.py does at module load ----------
def _stub(name, **attrs):
    m = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(m, k, v)
    m.__getattr__ = lambda attr: type(attr, (object,), {})  # any other name -> dummy class
    sys.modules[name] = m
    return m


class _Tx:
    def atomic(self, *a, **k):
        class _C:
            def __enter__(s):
                return s

            def __exit__(s, *a):
                return False

        return _C()


_stub("django")
_stub("django.db", close_old_connections=lambda *a, **k: None, transaction=_Tx())
for _n in ("apps", "apps.channels", "apps.channels.models", "apps.m3u", "apps.m3u.models"):
    _stub(_n)

_HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location("failovarr_plugin", os.path.join(_HERE, "..", "plugin.py"))
fv = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fv)

REGION = {"US", "EN"}


# --- quality tier logic ---------------------------------------------------------
def test_is_quality_token():
    for t in ("HD", "4K", "UHD", "FHD", "SD", "720P", "1080", "2160", "3840", "60FPS", "RAW"):
        assert fv._is_quality_token(t), f"{t} should be quality"
    for t in ("CNN", "ESPN", "480", "66", "NEWS", "CW"):
        assert not fv._is_quality_token(t), f"{t} should NOT be quality"


def test_quality_suffix_canonical_tiers():
    assert fv._quality_suffix("US| ESPN HD") == ""          # HD is baseline
    assert fv._quality_suffix("US| ESPN") == ""             # untagged = baseline
    assert fv._quality_suffix("US| ESPN FHD") == ""         # FHD folds to baseline
    assert fv._quality_suffix("US| ESPN 4K") == "4K"
    assert fv._quality_suffix("US| ESPN UHD 3840P") == "4K"  # different decoration, same tier
    assert fv._quality_suffix("US| ESPN SD") == "SD"
    assert fv._quality_suffix("US| ESPN 60FPS") == "60FPS"
    assert fv._quality_suffix("US| ESPN 4K 60FPS") == "4K 60FPS"


# --- consolidation key ----------------------------------------------------------
def test_consolidation_key_separator_and_baseline():
    # Trex "US|" and Strong "US:" of the same HD channel collapse to one key.
    assert fv._consolidation_key("US| CNN HD", REGION, False) == \
           fv._consolidation_key("US: CNN HD", REGION, False)
    # HD/untagged both baseline -> same key.
    assert fv._consolidation_key("US| CNN", REGION, False) == \
           fv._consolidation_key("US| CNN HD", REGION, False)


def test_consolidation_key_tiers_and_plus():
    k_hd = fv._consolidation_key("US| ESPN HD", REGION, False)
    k_4k = fv._consolidation_key("US| ESPN 4K", REGION, False)
    assert k_hd != k_4k                      # HD vs 4K are distinct channels when merge is OFF
    assert fv._consolidation_key("US| ESPN UHD", REGION, False) == k_4k  # UHD == 4K tier
    # merge_quality ON drops the tier -> HD and 4K collapse.
    assert fv._consolidation_key("US| ESPN 4K", REGION, True) == \
           fv._consolidation_key("US| ESPN HD", REGION, True)
    # "+" brand stays distinct from base.
    assert fv._consolidation_key("US| AMC", REGION, False) != \
           fv._consolidation_key("US| AMC+", REGION, False)
    # non-region prefix (GO/PRIME) is kept in the key.
    assert "GO" in fv._consolidation_key("GO| FOO", REGION, False)


# --- group aliases --------------------------------------------------------------
def test_parse_group_aliases():
    assert fv._parse_group_aliases("SPORT = SPORTS") == {"SPORT": "SPORTS"}
    assert fv._parse_group_aliases("") == {}
    assert fv._parse_group_aliases("a -> b, c => d") == {"A": "b", "C": "d"}  # case-insensitive key
    assert fv._parse_group_aliases("news:NEWS") == {"NEWS": "NEWS"}


# --- subset profiles ------------------------------------------------------------
def test_parse_subset_profiles():
    assert fv._parse_subset_profiles("plex = ENTERTAINMENT") == [("plex", frozenset({"ENTERTAINMENT"}))]
    multi = dict(fv._parse_subset_profiles("plex = A, B\nkids = C"))
    assert multi["plex"] == frozenset({"A", "B"}) and multi["kids"] == frozenset({"C"})
    # semicolon separator + lowercase groups uppercased
    assert dict(fv._parse_subset_profiles("plex=entertainment; sports = SPORTS"))["sports"] == frozenset({"SPORTS"})
    # reserved (base/adult) names skipped; duplicate name keeps first
    assert fv._parse_subset_profiles("failovarr = X", reserved=("failovarr", "failovarr+18")) == []
    assert dict(fv._parse_subset_profiles("plex = A\nplex = B"))["plex"] == frozenset({"A"})


# --- group display / cleaning ---------------------------------------------------
def test_group_display_cleaning_and_alias():
    # superscripts + resolution + decorative glyph all stripped -> clean label
    assert fv._group_display("4K| RELAX ᵁᴴᴰ ³⁸⁴⁰ᴾ ☼", True) == "RELAX"
    # alias folds singular into plural
    assert fv._group_display("US| SPORT", True, {"SPORT": "SPORTS"}) == "SPORTS"
    # generic trailing word dropped when drop_suffix on
    assert fv._group_display("US| NEWS NETWORK", True) == "NEWS"
    # resolution-only group -> clean 4K label
    assert fv._group_display("4K| UHD 3840P", True) == "4K"


def test_city_local_group():
    assert fv._city_local_group("CITY| CW KDAF MIAMI RAW") == "CW"
    assert fv._city_local_group("CITY| PBS WGBH BOSTON") == "PBS"
    assert fv._city_local_group("CITY| TMO KVEA LA") == "Telemundo"
    assert fv._city_local_group("CITY| KICU SAN FRANCISCO") == "Independent"  # bare callsign
    assert fv._city_local_group("US| CNN HD") is None                        # not a CITY feed


# --- callsign / prefix / junk ---------------------------------------------------
def test_epg_callsign():
    assert fv._epg_callsign("KCEN-DT") == "KCEN"
    assert fv._epg_callsign("WHDC-LD") == "WHDC"
    assert fv._epg_callsign("Discovery Channel") is None


def test_callsign_stopwords():
    assert fv._callsign("ABC ATLANTA (KABC)") == "KABC"   # parenthesized wins
    assert fv._callsign("NBC WEST") is None               # WEST is a stopword, not a callsign
    assert fv._callsign("ABC KIDS") is None               # KIDS is a stopword


def test_country_prefix():
    assert fv._country_prefix("AR| FOO") == "AR"
    assert fv._country_prefix("US| CNN") == "US"
    assert fv._country_prefix("No Prefix Here") is None


def test_group_is_foreign_region_aware():
    RA = {"US", "EN"}
    # home-market prefixes kept
    assert fv._group_is_foreign("US| CNN HD", RA, True) is False
    assert fv._group_is_foreign("EN| BBC", RA, True) is False
    assert fv._group_is_foreign("AMAZON MOVIES", RA, True) is False  # no prefix -> kept
    # denylisted foreign prefixes filtered
    assert fv._group_is_foreign("DE| FOO", RA, True) is True
    assert fv._group_is_foreign("AR| BAR", RA, True) is True
    # non-Latin script is foreign regardless of prefix (e.g. an Arabic VOD category)
    assert fv._group_is_foreign("مسلسلات كرتون للكبار", RA, True) is True
    # region-aware (NOT US-hardcoded): add DE to the allowlist -> DE now kept
    assert fv._group_is_foreign("DE| FOO", {"US", "EN", "DE"}, True) is False


def test_group_is_foreign_alt_naming_forms():
    RA = {"US", "EN"}
    F = lambda n: fv._group_is_foreign(n, RA, True)  # noqa: E731
    # leading-pipe form
    assert F("|DE| ANIME FILME") is True
    assert F("|AR| TABII") is True
    assert F("|EN| 4K MOVIES") is False          # EN = home
    # spaced-dash form (accepted only for known codes)
    assert F("DE - FILME 2025/2026") is True
    assert F("AF - IROKO TV") is True
    assert F("BN - BENGALI") is True
    assert F("EN - COMEDY") is False             # EN = home
    assert F("SPORTS - EXTRA") is False          # non-code word is NOT a prefix
    # spelled-out country as the leading word
    assert F("DENMARK SPORT HD") is True
    assert F("GREECE NETFLIX") is True
    assert F("NORWAY GOLD RAW") is True
    assert F("CHINA ANIMATION") is True
    assert F("SOUTH AFRICA SERIES") is True      # two-word key
    assert F("ENGLISH SERIES") is False          # language, not a foreign country
    # BEE (beIN) denylist prefix
    assert F("BEE| AL KASS") is True


def test_group_is_foreign_no_false_positives():
    """US/English content must never read as foreign (would block auto-enable)."""
    RA = {"US", "EN"}
    F = lambda n: fv._group_is_foreign(n, RA, True)  # noqa: E731
    for n in (
        "US| CNN HD", "NETFLIX MOVIES", "DISNEY+ KIDS", "APPLE+ MOVIES",
        "HBO MAX SERIES", "PARAMOUNT+", "SOCCER FOOTBALL", "MARVEL MOVIES (MULTI)",
        "24/7 COMEDY VIP", "RATED R", "TOP IMDB/OSCAR MOVIES", "FORMULA 1 + MOTO GP",
        "|EN| NETFLIX", "EN - KIDS", "ENGLISH REALITY SERIES", "FOR ADULTS",
        "4K RELAX UHD 3840P", "Default Group", "Uncategorized",
    ):
        assert F(n) is False, n


def test_group_is_foreign_region_relax_all_forms():
    """A DE user keeps German content in every naming form; AR stays foreign."""
    RA = {"US", "EN", "DE"}
    F = lambda n: fv._group_is_foreign(n, RA, True)  # noqa: E731
    assert F("|DE| ANIME") is False
    assert F("DE - FILME") is False
    assert F("GERMANY DOKU SERIEN") is False
    assert F("|AR| TABII") is True


def test_homoglyph_fold():
    # Latin small-cap look-alikes NFKD leaves alone get mapped to ASCII
    assert fv._fold("cɪty") == "cIty"          # ɪ -> I
    assert fv._fold("ɴᴀ") == "NA"          # ɴᴀ -> NA
    # superscripts still fold as before (NFKD)
    assert fv._fold("ᴴᴰ") == "HD"          # ᴴᴰ -> HD
    # real non-Latin is NOT touched (must stay foreign-detectable)
    assert fv._is_non_latin("مسلسلات") is True
    assert fv._is_non_latin("Россия") is True


def test_duplicate_prefix_fold():
    RA = {"US", "EN"}
    DUP = {"TV"}
    # a TV|-dump channel now shares the key with the clean copy -> they merge
    k_clean = fv._consolidation_key("US| A&E HD", RA, True, frozenset())
    k_tv = fv._consolidation_key("TV| A&E RAW", RA, True, DUP)
    assert k_clean == k_tv
    # without the dup list, TV stays in the key (distinct channel) — the old behaviour
    assert fv._consolidation_key("TV| A&E RAW", RA, True, frozenset()) != k_clean
    # display name drops the folded prefix too
    assert fv._display_name("TV| A&E RAW", RA, DUP) == "A&E RAW"
    # a non-region, non-dup prefix (GO/PRIME) is still kept
    assert "GO" in fv._consolidation_key("GO| FOO", RA, True, DUP)


def test_govt_block():
    # header carries the true count; one bullet line per item; no tail under the cap
    items = [("trex", "live", "US| A"), ("strong", "vod", "EN - B")]
    lines = fv._govt_block("KEEP", items, cap=5)
    assert lines[0] == "KEEP — 2:"
    assert any("trex/live US| A" in l for l in lines)
    assert not any("more" in l for l in lines)
    # over the cap: exactly `cap` bullet lines + honest overflow tail
    many = [("t", "live", "G%d" % i) for i in range(10)]
    l2 = fv._govt_block("X", many, cap=3)
    assert l2[0] == "X — 10:"
    assert len([l for l in l2 if l.lstrip().startswith("•")]) == 3
    assert any("(+7 more)" in l for l in l2)
    # default cap is generous (>= 100)
    assert fv._GOV_LIST_CAP >= 100


def test_format_report_ppv_sections():
    data = {"status": "done", "mode": "ppv",
            "message": "PPV: 322 events added, 461 updated, 295 ended/removed",
            "stats": {"streams_scanned": 6174, "failover_pairs": 296, "single_source": 789,
                      "created": 322, "updated": 461, "pruned": 295}}
    out = fv.Plugin._format_report(None, data)
    ls = out.splitlines()
    assert ls[0] == "PPV events — done"
    assert "Events" in ls and "  • 322 added" in ls
    assert "Live now" in ls and "  • 1085 total" in ls   # derived pairs+single
    assert "?" not in out                                 # no reconcile-only ? rows
    # no bullet joins two metrics with a bare comma (parenthetical commas are fine)
    for ln in ls:
        if ln.lstrip().startswith("•"):
            import re as _re
            assert ", " not in _re.sub(r"\(.*?\)", "", ln)


def test_format_report_reconcile_sections():
    data = {"status": "done", "mode": "reconcile", "backup": "/x/b.json",
            "stats": {"streams_scanned": 100, "streams_skipped_foreign": 5, "keys_total": 80,
                      "failover_pairs": 30, "single_source": 50, "created": 1, "updated": 2, "pruned": 0},
            "epg": {"matched": 10, "sources": ["a", "b"]}, "health": {"existing": 80, "to_prune": 0}}
    out = fv.Plugin._format_report(None, data)
    ls = out.splitlines()
    assert ls[0] == "Reconcile — done"
    assert "  • 100 scanned" in ls and "  • 5 skipped (foreign)" in ls
    assert "  • +1 added" in ls and "  • -0 pruned" in ls
    assert "EPG" in ls and "Health" in ls and "Backup" in ls


def test_is_junk():
    assert fv._is_junk("##### FOX WISCONSIN #####") is True
    assert fv._is_junk("## MAX ESPN ##") is True          # 2-hash divider
    assert fv._is_junk("- NO EVENT STREAMING -") is True  # idle placeholder
    assert fv._is_junk("COMING SOON") is True
    assert fv._is_junk("US| CNN HD") is False


# --- plain runner (no pytest needed) --------------------------------------------
if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"PASS {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"FAIL {t.__name__}: {e}")
        except Exception as e:  # noqa
            failed += 1
            print(f"ERROR {t.__name__}: {type(e).__name__}: {e}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    sys.exit(1 if failed else 0)
