# coding:utf-8
# 提供selenium的driver
from selenium import webdriver
from selenium.webdriver.firefox.firefox_profile import FirefoxProfile
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
import util.headerutil
import util.proxyutil
import os
import sys
import configparser
import time
import requests
from lxml import etree

config = configparser.ConfigParser()
config.read("../config.properties")
firefox_path = config["general"]["firefox.path"]


# firefox 使用代理，自定义header
def get_firefox_driver(url, timeout=10):
    proxy = util.proxyutil.getdata5uproxy()
    ip = proxy['ip']
    address = ip[0:ip.find(":")]
    port = ip[ip.find(":") + 1:]
    print(proxy)
    driver = None
    try:
        profile = FirefoxProfile()
        profile.set_preference("network.proxy.type", 1)  # 1代表手动设置
        profile.set_preference("network.proxy.http", address)
        profile.set_preference("network.proxy.http_port", int(port))  # 端口必须是int类型，否则设置无效
        profile.set_preference('network.proxy.ssl', address)
        profile.set_preference('network.proxy.ssl_port', int(port))
        profile.set_preference("network.proxy.share_proxy_settings", True)
        profile.set_preference('permissions.default.image', 2)  # 不展示图片
        profile.set_preference("general.useragent.override", util.headerutil.get_header()['User-Agent'])  # 设置header
        profile.update_preferences()
        binary = FirefoxBinary(firefox_path)
        driver = webdriver.Firefox(firefox_profile=profile, firefox_binary=binary)
        driver.set_page_load_timeout(timeout)  # 设置超时
        driver.set_script_timeout(timeout)  # 设置超时
        driver.get(url)
        return driver
    except TimeoutException as r:
        print('page load timeout')
        if driver is None:
            close_driver(driver)
            return None
        if driver.page_source.strip() == '' or driver.page_source.strip() == '<html><head></head><body></body></html>':
            close_driver(driver)
            return None
        driver.execute_script('window.stop ? window.stop() : document.execCommand("Stop");')
        return driver
    except Exception as r:
        print("error %s" % str(r))
        try:
            driver.close()
        except Exception:
            print('close driver error')
        if "entity not found" in str(r):
            os.system("\"%s\"" % firefox_path)
            time.sleep(5)
        return None


def get_firefox_driver_without_proxy_without_header(url):
    try:
        profile = FirefoxProfile()
        profile.set_preference('permissions.default.image', 2)
        profile.update_preferences()
        binary = FirefoxBinary(firefox_path)
        driver = webdriver.Firefox(firefox_profile=profile, firefox_binary=binary)
        driver.set_page_load_timeout(10)
        driver.set_script_timeout(10)
        driver.get(url)
    except Exception as r:
        print("error %s" % str(r))
        if "entity not found" in str(r):
            os.system("\"%s\"" % firefox_path)
            time.sleep(5)
        driver = None
    return driver


# firefox driver，显示图片
def get_firefox_driver_with_image(url):
    proxy = util.proxyutil.getdata5uproxy()
    ip = proxy['ip']
    address = ip[0:ip.find(":")]
    port = ip[ip.find(":") + 1:]
    driver = None
    try:
        profile = FirefoxProfile()
        profile.set_preference("network.proxy.type", 1)  # 1代表手动设置
        profile.set_preference("network.proxy.http", address)
        profile.set_preference("network.proxy.http_port", int(port))  # 端口必须是int类型，否则设置无效
        profile.set_preference('network.proxy.ssl', address)
        profile.set_preference('network.proxy.ssl_port', int(port))
        profile.set_preference("network.proxy.share_proxy_settings", True)
        profile.set_preference('permissions.default.image', 1)  # 不展示图片
        profile.set_preference("general.useragent.override", util.headerutil.get_header()['User-Agent'])  # 设置header

        profile.update_preferences()
        binary = FirefoxBinary(firefox_path)
        driver = webdriver.Firefox(firefox_profile=profile, firefox_binary=binary)
        driver.set_page_load_timeout(15)  # 设置超时
        driver.set_script_timeout(15)  # 设置超时
        driver.get(url)
        return driver
    except TimeoutException:
        print('page load timeout')
        if driver is None:
            return None
        driver.execute_script('window.stop ? window.stop() : document.execCommand("Stop");')
        return driver
    except Exception as r:
        print("error %s" % str(r))
        try:
            driver.close()
        except:
            print('close driver error')
        if "entity not found" in str(r):
            os.system("\"%s\"" % firefox_path)
            time.sleep(5)
        return None


# firefox driver 没有代理
def get_firefox_driver_without_proxy(url):
    try:
        profile = webdriver.FirefoxProfile()
        profile.set_preference('permissions.default.image', 2)
        profile.set_preference("general.useragent.override", util.headerutil.get_header()['User-Agent'])  # 设置header
        profile.update_preferences()
        driver = webdriver.Firefox(firefox_profile=profile)
        driver.set_page_load_timeout(8)  # 设置超时
        driver.set_script_timeout(8)  # 设置超时
        driver.get(url)
        return driver
    except TimeoutException:
        if driver is None or driver.page_source is None or driver.page_source == '':
            try:
                driver.close()
                driver.quit()
            except:
                print('close error')
        print('页面加载过长')
        driver.execute_script('window.stop()')
        return driver
    except Exception as r:
        print("error %s" % str(r))
        if "entity not found" in str(r):
            os.system("\"%s\"" % firefox_path)
            time.sleep(5)
        try:
            driver.close()
            driver.quit()
        except:
            print('close error')
        return None


# firefox driver 没有代理有图片
def get_firefox_driver_without_proxy_with_img(url, timeout=30):
    try:
        profile_dir = "C:\\Users\\HP\\AppData\\Roaming\\Mozilla\\Firefox\\Profiles\\jzkcs1sr.default-release" \
                      "-1590391234035 "
        profile = webdriver.FirefoxProfile(profile_dir)
        profile.set_preference('permissions.default.image', 1)
        # profile.set_preference("general.useragent.override", util.headerutil.get_header()['User-Agent'])  # 设置header
        profile.update_preferences()
        binary = FirefoxBinary(firefox_path)

        driver = webdriver.Firefox(firefox_profile=profile, firefox_binary=binary)
        driver.set_page_load_timeout(timeout)  # 设置超时
        driver.set_script_timeout(timeout)  # 设置超时
        driver.get(url)
        return driver
    except TimeoutException:
        print('页面加载过长')
        driver.execute_script('window.stop()')
        return driver
    except Exception as r:
        print("error %s" % str(r))
        if "entity not found" in str(r):
            os.system("\"%s\"" % firefox_path)
            time.sleep(5)
        return None


def get_chrome_driver_without_proxy(url, timeout=30, executable_path=None, headless=False):
    try:
        header = util.headerutil.get_header()
        options = webdriver.ChromeOptions()
        if executable_path != None:
            options.add_argument('executable_path=%s' % executable_path)
        options.add_argument('lang=zh_CN.UTF-8')
        options.add_argument(
            'user-agent=%s' % header['User-Agent'])
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        if headless:
            options.add_argument('headless')
        driver = webdriver.Chrome(chrome_options=options)
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
            Object.defineProperty(navigator, 'webdriver', {
              get: () => undefined
            })
          """
        })
        driver.set_page_load_timeout(timeout)  # 设置超时
        driver.set_script_timeout(timeout)  # 设置超时
        driver.get(url)
        return driver
    except TimeoutException:
        print('页面加载过长')
        driver.execute_script('window.stop()')
        return driver
    except Exception as r:
        print("error %s" % str(r))
        if "entity not found" in str(r):
            os.system("\"%s\"" % firefox_path)
            time.sleep(5)
        return None


# 有代理的driver
def get_driver_use_proxy(url, timeout=35):
    proxy = None
    if url.find('https') > -1:
        proxy = util.proxyutil.gethttpsproxy()
    else:
        proxy = util.proxyutil.getproxy()

    ip = proxy['ip']
    address = ip[0:ip.find(":")]
    port = ip[ip.find(":") + 1:]

    try:
        firefox_options = webdriver.FirefoxOptions()
        firefox_options.headless = True
        profile = FirefoxProfile()
        profile.set_preference("network.proxy.type", 1)  # 1代表手动设置
        profile.set_preference("network.proxy.http", address)
        profile.set_preference("network.proxy.http_port", int(port))  # 端口必须是int类型，否则设置无效
        if "https" in url:
            profile.set_preference('network.proxy.ssl', "%s" % address)
            profile.set_preference('network.proxy.ssl_port', int(port))
        profile.set_preference("network.proxy.share_proxy_settings", True)
        profile.set_preference('permissions.default.image', 2)  # 不展示图片
        profile.set_preference("general.useragent.override", util.headerutil.get_header()['User-Agent'])  # 设置header

        profile.update_preferences()
        firefox_options.profile = profile
        binary = FirefoxBinary(firefox_path)
        driver = webdriver.Firefox(firefox_options=firefox_options, firefox_profile=profile, firefox_binary=binary)
        driver.set_page_load_timeout(timeout)  # 设置超时
        driver.set_script_timeout(timeout)  # 设置超时
        driver.get(url)
        return driver
    except TimeoutException:
        print('页面加载过长')
        driver.execute_script('window.stop()')
        return driver
    except Exception as r:
        print("error %s" % str(r))
        if "entity not found" in str(r):
            os.system("\"%s\"" % firefox_path)
            time.sleep(5)
        return None


# 加代理获取driver
def get_driver(url, timeout=35):
    return get_driver_use_proxy(url, timeout)


# 不加代理获取driver ，尽量不要使用这个
def get_driver_without_proxy(url):
    firefox_options = webdriver.FirefoxOptions()
    firefox_options.headless = True

    profile = FirefoxProfile()
    profile.set_preference('permissions.default.image', 2)  # 不展示图片
    profile.set_preference("general.useragent.override", util.headerutil.get_header()['User-Agent'])  # 设置header

    profile.update_preferences()
    firefox_options.profile = profile

    try:
        driver = webdriver.Firefox(firefox_options=firefox_options, firefox_profile=profile)
    except Exception as r:
        if "entity not found" in str(r):
            os.system("\"%s\"" % firefox_path)
            time.sleep(5)
        print(r)
        driver = None

    if driver is None:
        try:
            driver = webdriver.Firefox(firefox_options=firefox_options, firefox_profile=profile)
        except Exception as r:
            print(r)
            return None

    driver.set_page_load_timeout(10)
    try:
        driver.get(url)
        return driver
    except Exception as r:
        print("error %s" % str(r))

        try:
            driver.close()
        except:
            print('close error')
        return None


# 判断driver是否包含一个元素
def contains_element(driver, xpath):
    try:
        driver.find_element_by_xpath(xpath)
    except Exception as r:
        return False
    return True


def get_url_response_with_proxy(url):
    real_url = url
    if 'http' not in url:
        real_url = 'http://%s' % url

    r = None
    headers = util.headerutil.get_header()
    proxy = util.proxyutil.getproxy()
    proxies = {
        "HTTP": "http://%s" % proxy['ip'],
        "HTTPS": "https://%s" % proxy['ip']
    }
    try:
        r = requests.get(real_url, headers=headers, timeout=3, proxies=proxies)
    except Exception as t:
        print('get url error url = %s ,error = %s' % (url, str(t)))

    index = 0
    while r is None or r.text is None or r.text == '':
        time.sleep(1)
        if index > 5:
            break
        headers = util.headerutil.get_header()
        proxy = util.proxyutil.getproxy()
        proxies = {
            "HTTP": "http://%s" % proxy['ip'],
            "HTTPS": "https://%s" % proxy['ip']
        }
        try:
            r = requests.get(real_url, headers=headers, timeout=5, proxies=proxies)
        except Exception as t:
            print('get url error url = %s ,error = %s' % (url, str(t)))
        index += 1

    if r is None:
        return None

    return r


def get_url_content_text_post(url, formdata):
    real_url = url
    if 'http' not in url:
        real_url = 'http://%s' % url

    r = None
    headers = util.headerutil.get_header()
    # headers['Content-Type'] = 'application/x-www-form-urlencoded; charset=UTF-8'
    proxy = util.proxyutil.getproxy()
    proxies = {
        "HTTP": "http://%s" % proxy['ip'],
        "HTTPS": "https://%s" % proxy['ip']
    }
    try:
        r = requests.post(real_url, headers=headers, timeout=3, proxies=proxies, data=formdata, verify=False)
    except Exception as t:
        print('get url error url = %s ,error = %s' % (url, str(t)))

    index = 0
    while r is None or r.text is None or r.text == '':
        time.sleep(1)
        if index > 5:
            break
        headers = util.headerutil.get_header()
        proxy = util.proxyutil.getproxy()
        proxies = {
            "HTTP": "http://%s" % proxy['ip'],
            "HTTPS": "https://%s" % proxy['ip']
        }
        if r is not None:
            print(r.text)
        try:
            r = requests.post(real_url, headers=headers, timeout=10, proxies=proxies, data=formdata, verify=False)
        except Exception as t:
            print('get url error url = %s ,error = %s' % (url, str(t)))
        index += 1

    if r is None or r.text is None or r.text == '':
        return None

    r.encoding = r.apparent_encoding

    return r.text


# 获取静态网站的内容
def get_url_content_text(url, cookies=None, header=None):
    real_url = url
    if 'http' not in url:
        real_url = 'http://%s' % url
    r = None
    headers = util.headerutil.get_header()
    if header is not None:
        headers = header
    proxy = util.proxyutil.getproxy()
    proxies = {
        "HTTP": "http://%s" % proxy['ip'],
        "HTTPS": "https://%s" % proxy['ip']
    }
    try:
        if cookies is not None:
            r = requests.get(real_url, headers=headers, timeout=3, proxies=proxies, cookies=cookies)
        else:
            r = requests.get(real_url, headers=headers, timeout=3, proxies=proxies)
    except Exception as t:
        print('get url error url = %s ,error = %s' % (url, str(t)))

    index = 0
    while r is None or r.text is None or r.text == '':
        time.sleep(1)
        if index > 5:
            break
        headers = util.headerutil.get_header()
        if header is not None:
            headers = header
        proxy = util.proxyutil.getproxy()
        proxies = {
            "HTTP": "http://%s" % proxy['ip'],
            "HTTPS": "https://%s" % proxy['ip']
        }
        if r is not None:
            print(r.text)
        try:
            if cookies is not None:
                r = requests.get(real_url, headers=headers, timeout=3, proxies=proxies, cookies=cookies)
            else:
                r = requests.get(real_url, headers=headers, timeout=3, proxies=proxies)
        except Exception as t:
            print('get url error url = %s ,error = %s' % (url, str(t)))
        index += 1

    if r is None or r.text is None or r.text == '':
        return None

    r.encoding = r.apparent_encoding

    return r.text


# 获取静态网站的内容
def get_url_content_text_without_proxy(url, cookies=None):
    real_url = url
    if 'http' not in url:
        real_url = 'http://%s' % url

    r = None
    headers = util.headerutil.get_header()
    try:
        r = requests.get(real_url, headers=headers, timeout=3)
    except Exception as t:
        print('get url error url = %s ,error = %s' % (url, str(t)))

    index = 0
    while r is None or r.text is None or r.text == '':
        time.sleep(1)
        if index > 5:
            break
        headers = util.headerutil.get_header()
        try:
            r = requests.get(real_url, headers=headers, timeout=5)
        except Exception as t:
            print('get url error url = %s ,error = %s' % (url, str(t)))
        index += 1

    if r is None or r.text is None or r.text == '':
        return None

    r.encoding = r.apparent_encoding

    return r.text


# 获取静态网站的内容
def get_url_content(url):
    headers = util.headerutil.get_header()
    real_url = url
    if 'http' not in url:
        real_url = 'http://%s' % url

    r = None
    try:
        r = requests.get(real_url, headers=headers, timeout=5)
    except Exception as t:
        print('get url error url = %s ,error = %s' % (url, str(t)))

    if r is None or r.text is None or r.text == '':
        return None
    r.encoding = r.apparent_encoding
    if r.encoding == 'Windows-1254':
        r.encoding = 'gbk'
    sel = None
    try:
        sel = etree.HTML(r.text)
    except Exception as t:
        print('get html text error %s' % str(t))

    return sel


# 获取静态网站的内容
def get_url_content_with_proxy(url, timeout=5, noheader=False, cookies=None):
    real_url = url
    if 'http' not in url:
        real_url = 'http://%s' % url
    headers = util.headerutil.get_header()
    headers['Referer'] = 'http://www.faxin.cn'
    # headers['Cache-Control'] = 'no-cache'
    proxy = util.proxyutil.getproxy()
    if "https" in url:
        proxies = {
            "HTTPS": "https://%s" % proxy['ip']
        }
    else:
        proxies = {
            "HTTP": "http://%s" % proxy['ip']
        }
    index = 0
    html = None
    try:
        print(headers)
        if not noheader:
            if cookies is not None:
                print(cookies)
                print(proxies)
                html = requests.get(real_url, headers=headers, proxies=proxies, timeout=timeout, verify=False,
                                    cookies=cookies)
            else:
                html = requests.get(real_url, headers=headers, proxies=proxies, timeout=timeout, verify=False)
        else:
            if cookies is not None:
                html = requests.get(real_url, proxies=proxies, timeout=timeout, verify=False, cookies=cookies)
            else:
                html = requests.get(real_url, proxies=proxies, timeout=timeout, verify=False)


    except Exception as r:
        print('get %s error ,reason = %s' % (real_url, str(r)))

    if html is not None and html.status_code == 404:
        return None

    while html is None or html.text == '<html><head></head><body></body></html>' or "疑似黑客攻击" in html.text:

        headers = util.headerutil.get_header()
        headers['Cache-Control'] = 'no-cache'
        proxy = util.proxyutil.getproxy()
        if "https" in url:
            proxies = {
                "HTTPS": "https://%s" % proxy['ip']
            }
        else:
            proxies = {
                "HTTP": "http://%s" % proxy['ip']
            }
        try:
            if not noheader:
                if cookies is not None:
                    html = requests.get(real_url, headers=headers, proxies=proxies, timeout=timeout, verify=False,
                                        cookies=cookies)
                else:
                    html = requests.get(real_url, headers=headers, proxies=proxies, timeout=timeout, verify=False)
            else:
                if cookies is not None:
                    html = requests.get(real_url, proxies=proxies, timeout=timeout, verify=False, cookies=cookies)
                else:
                    html = requests.get(real_url, proxies=proxies, timeout=timeout, verify=False)
        except Exception as r:
            print('get %s error ,reason = %s' % (real_url, str(r)))

        index += 1
        if index > 10:
            break
        time.sleep(1)

    if html is None or html.text == '<html><head></head><body></body></html>':
        return None

    if html.status_code == 404:
        return 404

    html.encoding = html.apparent_encoding
    if html.encoding == 'Windows-1254':
        html.encoding = 'gbk'
    sel = etree.HTML(html.text)
    return sel


# 判断一个网址是否可以访问
def judge_url(url):
    headers = util.headerutil.get_header()
    real_url = url
    if 'http' not in url:
        real_url = 'http://%s' % url
    print(real_url)
    try:
        if 'https' in url:
            r = requests.get(real_url, timeout=5, verify=False, headers=headers, allow_redirects=True)
        else:
            r = requests.get(real_url, timeout=5, headers=headers, allow_redirects=True, verify=False)
    except Exception as r:
        print('get url error url = %s, error = %s ' % (url, str(r)))
        return False

    if r.status_code >= 400:
        return False
    r.encoding = r.apparent_encoding

    if "该信息不存在" in r.text:
        return False
    return True


def close_driver(driver):
    try:
        driver.close()
    except Exception as r:
        print(str(r))


# 获取真实网址
def get_real_url(url, try_count=1):
    if try_count > 5:
        return url
    headers = util.headerutil.get_header()
    proxy = util.proxyutil.getproxy()
    proxies = {
        "HTTP": "http://%s" % proxy['ip'],
        "HTTPS": "https://%s" % proxy['ip']
    }
    try:
        rs = requests.get(url, headers=headers, proxies=proxies, timeout=3)
        if rs.status_code > 400:
            return get_real_url(url, try_count + 1)
        return rs.url
    except:
        return get_real_url(url, try_count + 1)


# 获取真实标题
def get_url_title(url, try_count=1):
    print(url)
    if try_count > 5:
        return None
    headers = util.headerutil.get_header()
    proxy = util.proxyutil.getproxy()
    proxies = {
        "HTTP": "http://%s" % proxy['ip'],
        "HTTPS": "https://%s" % proxy['ip']
    }
    try:
        rs = requests.get(url, headers=headers, proxies=proxies, timeout=3)
        if rs.status_code > 400:
            return get_url_title(url, try_count + 1)
        rs.encoding = rs.apparent_encoding
        sel = etree.HTML(rs.text)
        title = sel.xpath("//head/title")[0].text
        if "_" in title:
            return title.split("_")[0]
        return title
    except:
        return get_url_title(url, try_count + 1)
