import aiohttp
import asyncio
import json
import time
from cmp_version import VersionString


class BranchAnalyzer:

    def __init__(self, first_branch: str, second_branch: str):
        self.__first_branch = first_branch
        self.__second_branch = second_branch
        self.__url = 'https://rdb.altlinux.org/api/export/branch_binary_packages/'

    async def get_branch_data(self, branch: str) -> tuple[dict, bool]:
        """
        Asynchronous get packages data
        """
        async with aiohttp.ClientSession() as session:
            async with session.get(self.__url + branch, ) as response:
                return await response.json(), response.status == 200

    async def analyse(self) -> None:
        """
        Main function
        """
        if not self.__first_branch or not self.__second_branch:
            return
        tasks = self.get_branch_data(self.__first_branch), self.get_branch_data(self.__second_branch)
        first_branch_data, second_branch_data = await asyncio.gather(*tasks)
        if not first_branch_data[1] or not second_branch_data[1]:
            return
        first_branch_data, second_branch_data = first_branch_data[0], second_branch_data[0]
        only_in_first = BranchAnalyzer.only_in_one_branch(first_branch_data['packages'], second_branch_data['packages'])
        only_in_second = BranchAnalyzer.only_in_one_branch(second_branch_data['packages'],
                                                           first_branch_data['packages'])
        greater_version = BranchAnalyzer.get_greater_version_in_first(first_branch_data['packages'],
                                                                      second_branch_data['packages'])
        result = {
            'only in ' + self.__first_branch: only_in_first,
            'only in ' + self.__second_branch: only_in_second,
            'version-release greater in ' + self.__first_branch: greater_version
        }
        print(json.dumps(result))

    @staticmethod
    def get_greater_version_in_first(first_branch_packages: list[dict], second_branch_packages: list[dict]) -> list[dict]:
        """
        Compare packages version-release. Return fist branch packages which version-release greater than in second.
        """
        result = []
        packages_map = {(package['name'] + package['arch']).strip(): package for package in first_branch_packages}
        for package in second_branch_packages:
            second_branch_package = packages_map.get((package['name'] + package['arch']).strip() )
            if second_branch_package:
                version1 = VersionString(package['version'] + '-' + package['release'])
                version2 = VersionString(second_branch_package['version'] + '-' + second_branch_package['release'])
                if version1 > version2:
                    result.append(package)
        return result

    @staticmethod
    def only_in_one_branch(first_branch_packages: list[dict], second_branch_packages: list[dict]) -> list[dict]:
        """
        Get packages only in one branch. Return list of packages only in first branch
        """
        packages_map = {(package['name'] + package['arch']).strip(): package for package in first_branch_packages}
        first_branch_packages = frozenset(packages_map.keys())
        second_branch_packages = frozenset([(package['name'] + package['arch']).strip() for package in second_branch_packages])
        return [packages_map[package] for package in first_branch_packages.difference(second_branch_packages)]


if __name__ == '__main__':
    analyzer = BranchAnalyzer('sisyphus', 'p10')
    asyncio.run(analyzer.analyse())
