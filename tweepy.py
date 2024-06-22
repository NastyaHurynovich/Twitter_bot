import time, os

import yaml
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options

with open('twitter.yaml') as f:
    twitter_config = yaml.safe_load(f)


class Twitterbot:

    def __init__(self, email, password):
        """Constructor

        Arguments:
            email {string} -- registered twitter email
            password {string} -- password for the twitter account
        """

        self.email = email
        self.password = password
        # initializing chrome options
        chrome_options = Options()

        # adding the path to the chrome driver and
        # integrating chrome_options with the bot
        self.bot = webdriver.Chrome(
            executable_path=os.path.join(os.getcwd(), 'chromedriver'),
            options=chrome_options
        )

    def contains(self, list, filter):
        for x in list:
            if filter(x):
                return True
        return False

    def login(self):
        """
            Method for signing in the user
            with the provided email and password.
        """

        bot = self.bot
        # fetches the login page
        bot.get('https://twitter.com/login')
        # adjust the sleep time according to your internet speed
        time.sleep(3)

        email = bot.find_element(By.CSS_SELECTOR, 'input[name="text"]')

        # sends the email to the email input
        email.send_keys(self.email)

        email.send_keys(Keys.RETURN)

        time.sleep(3)
        try:
            password = bot.find_element(By.CSS_SELECTOR, 'input[name="text"]')
            # sends the password to the password input
            password.send_keys(twitter_config["twitter_account_name"])
            # executes RETURN key action
            password.send_keys(Keys.RETURN)
        except:
            print("")

        time.sleep(3)
        password = bot.find_element(By.CSS_SELECTOR, 'input[name="password"]')
        # sends the password to the password input
        password.send_keys(self.password)
        # executes RETURN key action
        password.send_keys(Keys.RETURN)

        time.sleep(2)

    def logout(self):
        """
            Method for signing in the user
            with the provided email and password.
        """

        bot = self.bot
        # fetches the login page
        bot.get('https://twitter.com/logout')
        # adjust the sleep time according to your internet speed
        time.sleep(3)
        bot.find_element(By.CSS_SELECTOR, 'div[data-testid="confirmationSheetConfirm"]').click()
        time.sleep(3)

    def get_users(self, names):

        bot = self.bot
        users = []
        for name in names:
            name = name[1:]
            # fetches the latest tweets with the provided hashtag
            bot.get(
                'https://twitter.com/' + name
            )
            user = {
                "name": name,
                "url": "https://twitter.com/" + name
            }

            time.sleep(3)
            user_id = bot.find_element(By.CSS_SELECTOR, 'div[aria-label="Follow @' + name + '"]').get_attribute(
                "data-testid")
            user_id = user_id.split('-')[0]
            user["id"] = user_id
            time.sleep(3)

            user["friends_list"] = self.get_user_friends(user["name"])

            users.append(user)

        return users

    def get_user_friends(self, name):
        bot = self.bot
        bot.get(
            'https://twitter.com/' + name
        )
        time.sleep(2)
        bot.find_element(By.CSS_SELECTOR, 'a[href="/' + name + '/following"]').click()
        time.sleep(2)

        friends_list = []

        friends_elements = bot.find_elements(By.CSS_SELECTOR,
                                             'div[data-testid="cellInnerDiv"] div[data-testid="UserCell"] div[role="button"]')
        for friend in friends_elements:
            friend_id = friend.get_attribute("data-testid").split("-")[0]
            friend_name = friend.get_attribute("aria-label").split(" ")[1]
            if self.contains(friends_list, lambda x: x["id"] == friend_id) is not True:
                friends_list.append({
                    "id": friend_id,
                    "name": friend_name,
                    "url": 'https://twitter.com/' + friend_name[1:]
                })

        last_height = bot.execute_script("return document.body.scrollHeight")
        while True:
            time.sleep(1)

            # Scroll down to bottom
            bot.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            # Wait to load page
            time.sleep(1)
            new_height = bot.execute_script("return document.body.scrollHeight")
            if new_height <= last_height:
                break
            # Calculate new scroll height and compare with last scroll height
            last_height = new_height

            # get friends_elements element
            friends_elements = bot.find_elements(By.CSS_SELECTOR,
                                                 'div[data-testid="cellInnerDiv"] div[data-testid="UserCell"] div[role="button"]')
            for friend in friends_elements:
                friend_id = friend.get_attribute("data-testid").split("-")[0]
                friend_name = friend.get_attribute("aria-label").split(" ")[1]
                if self.contains(friends_list, lambda x: x["id"] == friend_id) is not True:
                    friends_list.append({
                        "id": friend_id,
                        "name": friend_name,
                        "url": 'https://twitter.com/' + friend_name[1:]
                    })

        return friends_list
