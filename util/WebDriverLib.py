#!/usr/local/bin/python
# coding: utf-8
import os

from selenium.webdriver.support import expected_conditions as EC
import selenium.webdriver.support.ui as ui


class WebDriverLib:
    def __init__(self, driver):
        self.wait = ui.WebDriverWait(driver, 10)

    def waitToFind(self, by):
        return self.wait.until(
            EC.presence_of_element_located(by)
        )

    def waitToVisible(self, by):
        return self.wait.until(
            EC.visibility_of_element_located(by)
        )

    def waitToLeave(self, by):
        return self.wait.until(
            EC.invisibility_of_element_located(by)
        )

    def take_screenshot_on_item(self, driver, element, path, delay=10, padding=60):
        import time
        check_and_mkdir(path)
        # time.sleep(10)
        driver.execute_script("arguments[0].scrollIntoView();", element)
        driver.execute_script("""
            (function () {
                var y = scrollY;
                var step = 100;
                window.scroll(0, y);

                function f() {
                    if (y < arguments[0]) {
                        y += step;
                        window.scroll(0, y);
                        setTimeout(f, 100);
                    } else {
                        document.title += "scroll-done";
                    }
                }

                setTimeout(f, 1000);
            })();
        """, element.location['y']+element.size['height'])

        for i in range(300):
            if "scroll-done" in driver.title:
                break
            time.sleep(1)

        print('wait ',delay,'seconds for images loading')
        time.sleep(delay)
        print('scroll to element')
        location = element.location
        size = element.size
        window_size = driver.get_window_size()

        from selenium.webdriver.common.by import By
        body_height = driver.find_element(By.TAG_NAME, "body").size['height']

        remain_height = size['height']
        window_height = window_size['height'] - 90
        step = window_height
        # padding = 60
        left = location['x'] - padding
        right = location['x'] + size['width'] + padding
        top = location['y']
        cur_bottom = location['y']
        screenshot_index = 0
        if remain_height <= window_height:
            filename = path + '/screenshot'+str(screenshot_index)+'.png'
            if top > window_height - size['height']:
                # scroll to put element at the bottom of window
                driver.execute_script("scroll(0,arguments[0]);", top + size['height'] - window_height)
                driver.save_screenshot(filename) # saves screenshot of entire page
                scop_to(filename, left, window_height-size['height'], right, window_height)
            else:
                driver.save_screenshot(filename) # saves screenshot of entire page
                scop_to(filename, left, element.location['y'], right, element.location['y'] + size['height'])
        else:
            driver.execute_script("arguments[0].scrollIntoView();", element)
            while True:
                time.sleep(0.1)
                print('location size: ',element.location['x'],element.location['y'],element.size['width'],element.size['height'])
                filename = path + '/screenshot'+str(screenshot_index)+'.png'
                # if remain_height > step:
                driver.save_screenshot(filename) # saves screenshot of entire page
                # driver.save_screenshot(filename+'.raw')
                remain_height -= step
                if remain_height <= 0:
                    # driver.save_screenshot(filename+'.raw') # saves screenshot of entire page
                    scop_to(filename, left, window_height-step, right, window_height)
                    break
                else:
                    # remain_height -= step
                    scop_to(filename, left, 0, right, step)
                if remain_height < step:
                    step = remain_height
                driver.execute_script("scrollBy(0,arguments[0]);", step)
                screenshot_index += 1
                cur_bottom += step
                print('save ',filename)
        driver.execute_script("document.title = document.title.replace('scroll-done','')")

    def take_screenshot_on_element(self, driver, item, path):
        # import time
        # t = time.time()
        # fullname = '/tmp/'+'webdriverlib'+t+'.png'
        check_and_mkdir(path)
        filename = path+'/screenshot.png'
        driver.save_screenshot(filename)
        left = item.location['x']
        right = left + item.size['width']
        top = item.location['y']
        bottom = top + item.size['height']
        scop_to(filename, left, top, right, bottom)

    def wait_for_img_loading_finished(self, driver, delay=10):
        import time
        # driver.execute_script("arguments[0].scrollIntoView();")
        driver.execute_script("""
            (function () {
                var y = scrollY;
                var step = 100;
                window.scroll(0, 0);

                function f() {
                    if (y < document.body.scrollHeight) {
                        y += step;
                        window.scroll(0, y);
                        setTimeout(f, 100);
                    } else {
                        window.scroll(0,0);
                        document.title += "scroll-done";
                    }
                }

                setTimeout(f, 1000);
            })();
        """)

        for i in range(300):
            if "scroll-done" in driver.title:
                break
            time.sleep(1)
        time.sleep(delay)

def scop_to(filename, left, top, right, bottom):
    from PIL import Image
    im = Image.open(filename)  # uses PIL library to open image in memory
    im = im.crop((left, top, right, bottom))  # defines crop points
    im.save(filename)  # saves new cropped image
    print('crop', filename)

def check_and_mkdir(filename):
    if not os.path.isdir(filename):
        os.mkdir(filename)