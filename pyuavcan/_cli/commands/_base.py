# Copyright (c) 2019 UAVCAN Consortium
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel@uavcan.org>

import abc
import typing
import argparse
import pyuavcan
from ._subsystems import SubsystemFactory as SubsystemFactory


class Command(abc.ABC):
    """
    Base command class.
    The constructor shall have no required arguments.
    """

    @property
    @abc.abstractmethod
    def names(self) -> typing.Sequence[str]:
        """
        Command names ordered by preference; first name is the main name. At least one element is required.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def help(self) -> str:
        """
        Documentation help string. Limit the lines to 80 characters max.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def examples(self) -> typing.Optional[str]:
        """
        Set of human-readable usage examples; None if not defined. Limit the lines to 80 characters max.
        """
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def subsystem_factories(self) -> typing.Sequence[SubsystemFactory]:
        """
        Subsystems that will be instantiated before the command is executed.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def register_arguments(self, parser: argparse.ArgumentParser) -> None:
        """
        Populates the specified parser instance with command arguments.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def execute(self, args: argparse.Namespace, subsystems: typing.Sequence[object]) -> int:
        """
        Runs the command with the specified arguments and the subsystems constructed from the predefined factories.
        """
        raise NotImplementedError

    def __repr__(self) -> str:
        return pyuavcan.util.repr_attributes(self, names=self.names)
