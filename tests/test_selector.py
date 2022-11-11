

from ftl_diff.rule import build_rule_selector



def test_selector_normal():
    assert r"root['key'][\d+]" == build_rule_selector('root.key.index')


def test_selector_no_root():
    assert r"['key'][\d+]" == build_rule_selector('key.index')


def test_selector_many_keys():
    assert r"root['key']['foo']['bar']" == build_rule_selector('root.key.foo.bar')

def test_selector_many_indexes():
    assert r"root['key'][\d+][\d+]" == build_rule_selector('root.key.index.index')

def test_selector_name():
    assert r"['name']" == build_rule_selector('name')
