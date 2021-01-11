# This module is specifically designed to raise ImportError when imported. This is needed for testing purposes.

# noinspection PyUnresolvedReferences
import nonexistent_module_should_raise_import_error  # type: ignore  # pylint: disable=import-error
