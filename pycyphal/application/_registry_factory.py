# Copyright (c) 2021 OpenCyphal
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@opencyphal.org>

from __future__ import annotations
import os
from typing import Callable, Optional, Union, List, Dict
from pathlib import Path
import logging
from . import register


EnvironmentVariables = Union[Dict[str, bytes], Dict[str, str], Dict[bytes, bytes]]


class SimpleRegistry(register.Registry):
    def __init__(
        self,
        register_file: Union[None, str, Path] = None,
        environment_variables: Optional[EnvironmentVariables] = None,
    ) -> None:
        from .register.backend.dynamic import DynamicBackend
        from .register.backend.static import StaticBackend

        self._backend_static = StaticBackend(register_file)
        self._backend_dynamic = DynamicBackend()

        if environment_variables is None:
            try:
                environment_variables = os.environb  # type: ignore
            except AttributeError:  # pragma: no cover
                environment_variables = os.environ  # type: ignore

        assert environment_variables is not None
        self._environment_variables: Dict[str, bytes] = {
            (k if isinstance(k, str) else k.decode()): (v if isinstance(v, bytes) else v.encode())
            for k, v in environment_variables.items()
        }
        super().__init__()

        self._update_from_environment_variables()

    @property
    def backends(self) -> List[register.backend.Backend]:
        return [self._backend_static, self._backend_dynamic]

    @property
    def environment_variables(self) -> Dict[str, bytes]:
        return self._environment_variables

    def _create_static(self, name: str, value: register.Value) -> None:
        _logger.debug("%r: Create static %r = %r", self, name, value)
        self._backend_static[name] = value

    def _create_dynamic(
        self,
        name: str,
        getter: Callable[[], register.Value],
        setter: Optional[Callable[[register.Value], None]],
    ) -> None:
        _logger.debug("%r: Create dynamic %r from getter=%r setter=%r", self, name, getter, setter)
        self._backend_dynamic[name] = getter if setter is None else (getter, setter)

    def _update_from_environment_variables(self) -> None:
        for name in self:
            env_val = self.environment_variables.get(register.get_environment_variable_name(name))
            if env_val is not None:
                _logger.debug("Updating register %r from env: %r", name, env_val)
                reg_val = self[name]
                reg_val.assign_environment_variable(env_val)
                self[name] = reg_val


def make_registry(
    register_file: Union[None, str, Path] = None,
    environment_variables: Optional[EnvironmentVariables] = None,
) -> register.Registry:
    """
    Construct a new instance of :class:`pycyphal.application.register.Registry`.
    Complex applications with uncommon requirements may choose to implement Registry manually
    instead of using this factory.

    See also: standard RPC-service ``uavcan.register.Access``.

    :param register_file:
        Path to the registry file; or, in other words, the configuration file of this application/node.
        If not provided (default), the registers of this instance will be stored in-memory (volatile configuration).
        If path is provided but the file does not exist, it will be created automatically.
        See :attr:`Node.registry`.

    :param environment_variables:
        During initialization, all registers will be updated based on the environment variables passed here.
        This dict is used to initialize :attr:`pycyphal.application.register.Registry.environment_variables`.
        Registers that are created later using :meth:`pycyphal.application.register.Registry.setdefault`
        will use these values as well.

        If None (which is default), the value is initialized by copying :data:`os.environb`.
        Pass an empty dict here to disable environment variable processing.

    :raises:
        - :class:`pycyphal.application.register.ValueConversionError` if a register is found but its value
          cannot be converted to the correct type, or if the value of an environment variable for a register
          is invalid or incompatible with the register's type
          (e.g., an environment variable set to ``Hello world`` cannot be assigned to register of type ``real64[3]``).
    """
    return SimpleRegistry(register_file, environment_variables)


_logger = logging.getLogger(__name__)
