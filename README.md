# python_logging_yandex
## Установка
```shell
pip install git+https://github.com/Gafret/Python-Yandex-Logger.git
```
## Использование

```python
import time
import logging

from src.logging_yandex.handlers import YandexCloudHandler

handler = YandexCloudHandler(
    credentials={
        "token": "OAUTH_TOKEN",
        "iam_token": "IAM_TOKEN"
    },
    resource_id="id",
    resource_type="type",
    log_group_id="id",
    folder_id="id",  # use either group_id or folder_id, not both
    log_batch_size=5,  # optional
    commit_period=5,  # optional 
)

logger = logging.getLogger("yandex-logger")
logger.addHandler(handler)
time.sleep(6)
logger.warning(
    "Oh no...",
    extra={"json_payload": {"tags": "service"}}
)
```

Вышеуказанный код отошлет лог на ваш Yandex Cloud Logger с указанным текстом сообщения и элементами словаря `"payload"`
как дополнительными аттрибутами

`time.sleep(6)` так как выставлен `commit_period=5` - это значит, что при незаполненном до 5 буфере 
логов (`log_batch_size=5`) мы отправим накопленные логи только если уже прошел выставленный период  
