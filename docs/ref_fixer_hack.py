"""
========================================  THIS IS A DIRTY HACK  ========================================

I've constructed this Sphinx extension as a quick and dirty "solution" to the problem of broken cross-linking.

The problem is that Autodoc fails to realize that an entity, say, pycyphal.transport._session.InputSession is
exposed to the user as pycyphal.transport.InputSession, and that the original name is not a part of the API
and it shouldn't even be mentioned in the documentation at all. I've described this problem in this Sphinx issue
at https://github.com/sphinx-doc/sphinx/issues/6574. Since the original name is not exported, Autodoc can't find
it in the output and generates no link at all, requiring the user to search manually instead of just clicking on
stuff.

The hack is known to occasionally misbehave and produce incorrect links at the output, but hey, it's a hack.
Someone should just fix Autodoc instead of relying on this long-term. Please.
"""

import re
import os
import typing

import sphinx.application
import sphinx.environment
import sphinx.util.nodes
import docutils.nodes


_ACCEPTANCE_PATTERN = r".*([a-zA-Z][a-zA-Z0-9_]*\.)+_[a-zA-Z0-9_]*\..+"
_REFTYPES = "class", "meth", "func"

_replacements_made: typing.List[typing.Tuple[str, str]] = []


def missing_reference(
    app: sphinx.application.Sphinx,
    _env: sphinx.environment.BuildEnvironment,
    node: docutils.nodes.Element,
    contnode: docutils.nodes.Node,
) -> typing.Optional[docutils.nodes.Node]:
    old_reftarget = node["reftarget"]
    if node["reftype"] in _REFTYPES and re.match(_ACCEPTANCE_PATTERN, old_reftarget):
        new_reftarget = re.sub(r"\._[a-zA-Z0-9_]*", "", old_reftarget)
        if new_reftarget != old_reftarget:
            _replacements_made.append((old_reftarget, new_reftarget))
            attrs = contnode.attributes if isinstance(contnode, docutils.nodes.Element) else {}
            try:
                old_refdoc = node["refdoc"]
            except KeyError:
                return None
            new_refdoc = old_refdoc.rsplit(os.path.sep, 1)[0] + os.path.sep + new_reftarget.rsplit(".", 1)[0]
            return sphinx.util.nodes.make_refnode(
                app.builder,
                old_refdoc,
                new_refdoc,
                node.get("refid", new_reftarget),
                docutils.nodes.literal(new_reftarget, new_reftarget, **attrs),
                new_reftarget,
            )
    return None


def doctree_resolved(_app: sphinx.application.Sphinx, doctree: docutils.nodes.document, _docname: str) -> None:
    def predicate(n: docutils.nodes.Node) -> bool:
        if isinstance(n, docutils.nodes.FixedTextElement):
            is_text_primitive = len(n.children) == 1 and isinstance(n.children[0], docutils.nodes.Text)
            if is_text_primitive:
                return is_text_primitive and re.match(_ACCEPTANCE_PATTERN, n.children[0].astext())
        return False

    def substitute_once(text: str) -> str:
        out = re.sub(r"\._[a-zA-Z0-9_]*", "", text)
        _replacements_made.append((text, out))
        return out

    # The objective here is to replace all references to hidden objects with their exported aliases.
    # For example: pycyphal.presentation._typed_session._publisher.Publisher --> pycyphal.presentation.Publisher
    for node in doctree.traverse(predicate):
        assert isinstance(node, docutils.nodes.FixedTextElement)
        node.children = [docutils.nodes.Text(substitute_once(node.children[0].astext()))]


def setup(app: sphinx.application.Sphinx):
    app.connect("missing-reference", missing_reference)
    app.connect("doctree-resolved", doctree_resolved)
    return {
        "parallel_read_safe": True,
    }
