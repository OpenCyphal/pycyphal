"""
========================================  THIS IS A DIRTY HACK  ========================================

I've constructed this Sphinx extension as a quick and dirty "solution" to the problem of broken cross-linking.

The problem is that Autodoc fails to realize that an entity, say, pyuavcan.transport._session.InputSession is
exposed to the user as pyuavcan.transport.InputSession, and that the original name is not a part of the API
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


_ACCEPTANCE_PATTERN = r'([a-zA-Z][a-zA-Z0-9_]*\.)+_[a-zA-Z0-9_]*\..+'
_REFTYPES = 'class', 'meth', 'func'


def missing_reference(app:      sphinx.application.Sphinx,
                      _env:     sphinx.environment.BuildEnvironment,
                      node:     docutils.nodes.Element,
                      contnode: docutils.nodes.Node) -> typing.Optional[docutils.nodes.Node]:
    old_reftarget = node['reftarget']
    if node['reftype'] in _REFTYPES and re.match(_ACCEPTANCE_PATTERN, old_reftarget):
        new_reftarget = re.sub(r'\._[a-zA-Z0-9_]*', '', old_reftarget)
        if new_reftarget != old_reftarget:
            attrs = contnode.attributes if isinstance(contnode, docutils.nodes.Element) else {}
            new_refdoc = node['refdoc'].rsplit(os.path.sep, 1)[0] + os.path.sep + new_reftarget.rsplit('.', 1)[0]
            return sphinx.util.nodes.make_refnode(app.builder,
                                                  node['refdoc'],
                                                  new_refdoc,
                                                  node.get('refid', new_reftarget),
                                                  docutils.nodes.literal(new_reftarget, new_reftarget, **attrs),
                                                  new_reftarget)
    return None


def setup(app: sphinx.application.Sphinx):
    # app.add_config_value('ref_fixer_pattern', r'.*', True, (str,))
    app.connect('missing-reference', missing_reference)
    return {}
