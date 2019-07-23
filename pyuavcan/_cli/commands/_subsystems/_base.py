#
# Copyright (c) 2019 UAVCAN Development Team
# This software is distributed under the terms of the MIT License.
# Author: Pavel Kirienko <pavel.kirienko@zubax.com>
#

import abc
import argparse


class SubsystemFactory(abc.ABC):
    @abc.abstractmethod
    def register_arguments(self, parser: argparse.ArgumentParser) -> None:
        """
        Populates the provided parser with arguments specific to this subsystem.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def construct_subsystem(self, args: argparse.Namespace) -> object:
        """
        Constructs the product of this factory from the arguments.
        """
        raise NotImplementedError
