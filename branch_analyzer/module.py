import asyncio
import json

import aiohttp
import fire
from cmp_version import VersionString


class BranchAnalyzer:
    __url: str = 'https://rdb.altlinux.org/api/export/branch_binary_packages/'

    async def __get_branch_data(self, branch: str) -> tuple[dict, bool]:
        """
        Asynchronous get packages data
        """
        async with aiohttp.ClientSession(trust_env=True) as session:
            async with session.get(self.__url + branch) as response:
                return await response.json(), response.status == 200

    async def __analyse_branches(self, first_branch: str, second_branch: str) -> str:
        """
        Main function
        """
        if not first_branch or not second_branch:
            return ('first_branch' if not first_branch else "second_branch") + ' is empty'
        tasks = self.__get_branch_data(first_branch), self.__get_branch_data(second_branch)
        first_branch_data, second_branch_data = await asyncio.gather(*tasks)
        if not first_branch_data[1] or not second_branch_data[1]:
            return "unable to get data from " + (first_branch if not first_branch_data[1] else second_branch)
        first_branch_data, second_branch_data = first_branch_data[0], second_branch_data[0]
        only_in_first = self.__only_in_one_branch(first_branch_data['packages'], second_branch_data['packages'])
        only_in_second = self.__only_in_one_branch(second_branch_data['packages'], first_branch_data['packages'])
        greater_version = self.__get_greater_version_in_first(first_branch_data['packages'],
                                                              second_branch_data['packages'])
        result = {
            'only in ' + first_branch: only_in_first,
            'only in ' + second_branch: only_in_second,
            'version-release greater in ' + first_branch: greater_version
        }
        return json.dumps(result)

    def __get_greater_version_in_first(self, first_branch_packages: list[dict],
                                       second_branch_packages: list[dict]) -> list[dict]:
        """
        Compare packages version-release. Return first branch packages which version-release greater than in second.
        """
        result = []
        packages_map = {(package['name'] + package['arch']).strip(): package for package in first_branch_packages}
        for package in second_branch_packages:
            second_branch_package = packages_map.get((package['name'] + package['arch']).strip())
            if second_branch_package:
                version1 = VersionString(package['version'] + '-' + package['release'])
                version2 = VersionString(second_branch_package['version'] + '-' + second_branch_package['release'])
                if version1 > version2:
                    result.append(package)
        return result

    def __only_in_one_branch(self, first_branch_packages: list[dict], second_branch_packages: list[dict]) -> list[dict]:
        """
        Get packages only in one branch. Return list of packages only in first branch
        """
        packages_map = {(package['name'] + package['arch']).strip(): package for package in first_branch_packages}
        first_branch_packages = frozenset(packages_map.keys())
        second_branch_packages = frozenset([(package['name'] + package['arch']).strip() for package in second_branch_packages])
        return [packages_map[package] for package in first_branch_packages.difference(second_branch_packages)]

    def analyse(self, first_branch: str, second_branch: str):
        """
        Analyse branches packages. get data from 'https://rdb.altlinux.org/api/export/branch_binary_packages/'.
        :return: {
            'only in first_branch': list of packages available only in first branch,
            'only in second_branch': list of packages available only in second branch,
            'version-release greater in first_branch':list of first branch packages
             which version-release greater than in second.,
        }
        """
        if not first_branch or not second_branch:
            return ('first_branch' if not first_branch else "second_branch") + ' is empty'
        result = asyncio.run(self.__analyse_branches(first_branch, second_branch))
        return result


if __name__ == '__main__':
    obj = BranchAnalyzer()
    fire.Fire(obj)
