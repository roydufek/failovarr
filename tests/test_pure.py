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
