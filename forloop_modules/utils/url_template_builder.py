import difflib
from urllib.parse import urlparse


class UrlTemplateBuilder:
    """
    A class for constructing URL templates and formatting them with arguments.

    Args:
    ----
        url_1 (str): The first URL.
        url_2 (str): The second URL.

    Attributes:
    ----------
        templated_count (int): The count of templated arguments in the URL template.
        template_args (set): A set of argument names used in the URL template.
        url_template (str): The constructed URL template as a template string with `template_args` as placeholders.

    Methods:
    -------
        format_url: Format the URL template with the given arguments.
    """

    def __init__(self, url_1: str, url_2: str) -> None:
        self._url_1 = urlparse(url_1, scheme='http')
        self._url_2 = urlparse(url_2, scheme='http')

        self.templated_count = 0
        self.template_args = set()
        self.url_template = ''
        self._construct_url_template()

    def format_url(self, **kwargs) -> str:
        """Format the URL template with the given arguments."""
        if any(arg not in self.template_args for arg in kwargs):
            raise ValueError('One of the provided arguments is not the template argument')
        return self.url_template.format(**kwargs)

    def _construct_url_template(self) -> str:
        if self._url_1.netloc != self._url_2.netloc:
            raise ValueError('The URLs do not have the same netloc')

        path_template = self._build_path_template()
        query_template = self._build_query_template()

        self.url_template = f'{self._url_1.scheme}://{self._url_1.netloc}{path_template}?{query_template}'

        return self.url_template

    def _build_path_template(self) -> str:
        paths_1 = self._url_1.path.split('/')
        paths_2 = self._url_2.path.split('/')

        template_paths = []
        for arg_1, arg_2 in zip(paths_1, paths_2):
            if arg_1 == arg_2:
                template_paths.append(arg_1)
                continue

            self.templated_count += 1
            matcher = difflib.SequenceMatcher(None, arg_1, arg_2)
            opcodes = matcher.get_opcodes()
            final_arg = ''
            for tag, i1, i2, _, _ in opcodes:
                if tag == 'equal':
                    final_arg += arg_1[i1:i2]
                else:
                    self.template_args.add(f'path_{final_arg}')
                    final_arg += f"{{path_{final_arg}}}"
                    break  # Replace only the first difference in the path argument

            template_paths.append(final_arg)

        return '/'.join(template_paths)

    def _build_query_template(self) -> str:
        queries_1 = self._parse_query_params(self._url_1.query)
        queries_2 = self._parse_query_params(self._url_2.query)

        template_queries = {}
        for key in queries_1.keys() & queries_2.keys():  # Iterate over the common keys
            matcher = difflib.SequenceMatcher(None, queries_1[key], queries_2[key])
            opcodes = matcher.get_opcodes()
            final_value = ''
            for tag, i1, i2, _, _ in opcodes:
                if tag == 'equal':
                    final_value += queries_1[key][i1:i2]
                else:
                    self.templated_count += 1
                    final_value += f"{{query_{key}}}"
                    self.template_args.add(f'query_{key}')
                    break  # Replace only the first difference in the query argument

            template_queries[key] = final_value

        for key in queries_1.keys() ^ queries_2.keys():  # Iterate over the unique keys of both sets
            template_queries[key] = queries_1.get(key) or queries_2.get(key)

        query_strings = []
        # Sort query params in the order they appear in the first URL, then second URL
        unique_queries_2_keys = [key for key in queries_2.keys() if key not in queries_1]
        for key in [*queries_1.keys(), *unique_queries_2_keys]:
            query_strings.append(f'{key}={template_queries[key]}')

        return '&'.join(query_strings)

    def _parse_query_params(self, query: str) -> dict:
        """Build a dictionary of {'query_param': 'value'} from the query string."""
        query_dict = {}
        for arg in query.split('&'):
            if arg == '':
                continue
            key, value = arg.split('=')
            query_dict[key] = value

        return query_dict
