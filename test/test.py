import requests

url = "https://httpbin.org/get"
proxies = {
    "http": "http://192.187.126.98:19016",
}

response = requests.get(url, proxies=proxies)
print(response.text)
print(response.status_code)

