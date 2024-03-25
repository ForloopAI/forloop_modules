from forloop_modules.utils.url_template_builder import UrlTemplateBuilder


def test_url_template_builder():
    # Test case: Pagination is a path parameter, rest of the URL is identical
    url_1 = 'https://www.immobilier.ch/en/page-1?nb=false&gr=1'
    url_2 = 'https://www.immobilier.ch/en/page-2?nb=false&gr=1'
    template_builder = UrlTemplateBuilder(url_1, url_2)
    assert template_builder.url_template == 'https://www.immobilier.ch/en/page-{path_page-}?nb=false&gr=1'

    # Test case: Pagination is a query parameter, rest of the URL is identical
    url_3 = 'https://www.immobilier.ch/en?page=1&nb=false&gr=1'
    url_4 = 'https://www.immobilier.ch/en?page=2&nb=false&gr=1'
    template_builder = UrlTemplateBuilder(url_3, url_4)
    assert template_builder.url_template == 'https://www.immobilier.ch/en?page={query_page}&nb=false&gr=1'

    # Test case: Pagination is a query parameter, second URL has additional query parameters
    url_5 = 'https://www.immobilier.ch/en?page=1'
    url_6 = 'https://www.immobilier.ch/en?page=2&nb=false&gr=1'
    template_builder = UrlTemplateBuilder(url_5, url_6)
    assert template_builder.url_template == 'https://www.immobilier.ch/en?page={query_page}&nb=false&gr=1'

    # Test case: Pagination is a query parameter, second URL has additional query parameters before
    # and after the pagination
    url_7 = 'https://www.immobilier.ch/en?p=s126&page=1'
    url_8 = 'https://www.immobilier.ch/en?p=s126&page=2&nb=false&gr=1'
    template_builder = UrlTemplateBuilder(url_7, url_8)
    assert template_builder.url_template == 'https://www.immobilier.ch/en?p=s126&page={query_page}&nb=false&gr=1'

    # Test case: Pagination is a query parameter, second URL has additional query parameters before
    # and after the pagination, and a different query parameter value
    url_9 = 'https://www.immobilier.ch/en?p=s126&page=1&nb=true'
    url_10 = 'https://www.immobilier.ch/en?p=s126&page=2&nb=false&gr=1'
    template_builder = UrlTemplateBuilder(url_9, url_10)
    assert template_builder.url_template == 'https://www.immobilier.ch/en?p=s126&page={query_page}&nb={query_nb}&gr=1'

    # Test case: Pagination is a path parameter, second URL has additional query parameters and a
    # different path
    url_11 = 'https://www.immobilier.ch/en/page-1/zurich'
    url_12 = 'https://www.immobilier.ch/en/page-2/zurich?&nb=false&gr=1'
    template_builder = UrlTemplateBuilder(url_11, url_12)
    assert template_builder.url_template == 'https://www.immobilier.ch/en/page-{path_page-}/zurich?nb=false&gr=1'

    # Test case: URLs have both path and query parameters that change
    url_17 = 'https://www.example.com/bla1?param1=aaa'
    url_18 = 'https://www.example.com/bla2?param1=bbb'
    template_builder = UrlTemplateBuilder(url_17, url_18)
    assert template_builder.url_template == 'https://www.example.com/bla{path_bla}?param1={query_param1}'

    # Test case: URLs have multiple query parameters that change
    url_19 = 'https://www.example.com?param1=111&param2=aaa'
    url_20 = 'https://www.example.com?param1=222&param2=bbb'
    template_builder = UrlTemplateBuilder(url_19, url_20)
    assert template_builder.url_template == 'https://www.example.com?param1={query_param1}&param2={query_param2}'
