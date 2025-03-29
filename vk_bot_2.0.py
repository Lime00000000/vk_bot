import vk_api
from vk_api.longpoll import VkEventType, VkLongPoll
import sqlite3
import hashlib
from PIL import Image
import requests
from io import BytesIO


session = vk_api.VkApi(token=TOKEN)  # сюда токен бота(сообщества)
vk = session.get_api()


def send_message(user_id, message):
    session.method("messages.send", {
        "user_id": user_id,
        "message": message,
        "random_id": 0
    })


def dbInsert(user_id, group_id, data):
    con = sqlite3.connect('data/users.db')
    cursor = con.cursor()
    cursor.execute(f'INSERT INTO user(id, link, image) VALUES({user_id},"{group_id}","{';'.join(data)}")')
    con.commit()
    con.close()


def get_images_from_group(user_id, group_id, token):
    vk_session = vk_api.VkApi(token=token)
    vk = vk_session.get_api()

    try:
        group_info = vk.groups.getById(group_id=group_id)[0]

        posts = vk.wall.get(owner_id=-group_info['id'], count=100)['items']
        images = []

        for post in posts:
            attachments = post.get('attachments', [])
            for attachment in attachments:
                if attachment['type'] == 'photo':
                    photo = attachment['photo']
                    max_size_url = max(photo['sizes'], key=lambda size: size['width'])['url']
                    response = requests.get(max_size_url)
                    hash_img = hashlib.md5(Image.open(BytesIO(response.content)).tobytes()).hexdigest()  # из-за этого оно работает по 2 минуты
                    if hash_img in hash_set:
                        continue
                    hash_set.add(hash_img)
                    images.append(max_size_url)
        dbInsert(user_id, group_id, images)
        return 'Изображения загружены!'
    except Exception as e:
        return "Произошла ошибка"


def hash_from_db():
    con = sqlite3.connect('data/users.db')
    cursor = con.cursor()
    hash = set([el[0] for el in cursor.execute(f'SELECT start_hash FROM hash_sum').fetchall()])
    cursor.execute('DELETE from hash_sum')
    con.commit()
    con.close()
    return hash


def hash_to_db(hash_set):
    con = sqlite3.connect('data/users.db')
    cursor = con.cursor()
    hash_set = list(hash_set)
    for el in hash_set:
        cursor.execute(f'INSERT INTO hash_sum(start_hash) VALUES("{el}")')
    con.commit()
    con.close()


if __name__ == '__main__':
    hash_set = hash_from_db()
    for event in VkLongPoll(session).listen():
        if event.type == VkEventType.MESSAGE_NEW and event.to_me:
            text = event.text
            user_id = event.user_id
            group_id = text.split('/')[-1]
            access_token = TOKEN  # сюда токен для запроса https://vkhost.github.io я брал отсюда
            send_message(user_id, get_images_from_group(user_id, group_id, access_token))
            # при завершении он должен hash_to_db, но я не нашел как это по-человечески сделать