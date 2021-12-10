import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime
from glob import glob
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Union, List, Any, Dict


def setup_logger(
        filename: Union[str, Path],
        level: Union[str, int] = 'INFO',
) -> logging.Logger:
    """
    Настраивает логгирование с ротацией и автоматическим сжатием в zip.

    Args:
        filename: путь к файлу, куда будет записан лог
        level: уровень записей, которые будут записаны в файл
               (не влияет на вывод в консоли)

    Returns:
        logging.Logger: logger, настроенный на логирование в консоль и файл.
    """
    formatter = logging.Formatter('%(asctime)s  %(levelname)s: %(message)s')
    file_handler = RotatingFileHandler(
        filename=filename,
        maxBytes=1 * (2 ** 20),  # 1 MB
        backupCount=20,
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)

    _logger = logging.getLogger(__name__)
    _logger.setLevel(logging.DEBUG)
    _logger.addHandler(file_handler)
    _logger.addHandler(console_handler)

    return _logger


def get_config(filename: str) -> dict:
    """
    Возвращает все данными, содержащимися в конфиге.

    Args:
        filename (str): путь к json-конфигу.

    Returns:
        dict: словарь со всеми данными из конфига.ы
    """
    with open(filename, 'r') as f:
        config = json.load(f)
    return config


def get_file_list_from_json(filename: str) -> List[str]:
    """
    Читает список файлов из json.

    Args:
        filename: имя конфиг-файла.

    Returns:
        list: список путей к файлам, прочитанный из json-файла.
    """
    config = get_config(filename)

    files = config['files']
    assert isinstance(files, list), "files из конфига не является списком"
    return files


def get_hash(filename: str) -> str:
    """
    Возвращает хеш файла по его имени (в т.ч. пути к файлу).

    Args:
        filename: имя файла, хеш содержимого которого необходимо вычислить.

    Returns:
        str: строка, содержащая хеш файла.
    """
    file_hash = hashlib.md5()
    with open(filename, 'rb') as f:
        while chunk := f.read(32768):
            file_hash.update(chunk)

    return file_hash.hexdigest()


def get_modification_date(filename: str) -> str:
    """
    Возвращает время модификации файла в виде строки.

    Args:
        filename (str): имя файла, дату создания которого необходимо узнать.

    Returns:
        str: строка, содержащая дату и время последней модификации файла.
    """
    ti_m = os.path.getmtime(filename)
    m_ti = time.ctime(ti_m)
    t_obj = time.strptime(m_ti)
    str_time = time.strftime("%Y-%m-%d %H:%M:%S", t_obj)
    return str_time


def get_curr_files_data(filenames: List[str]) -> Dict[str, Any]:
    """
    Возвращает словарь, где каждому имени файла соответствует хеш и дата редактирования.

    Args:
        filenames (list[str]): пути к файлам

    Returns:
        dict: словарь с ключём-именем файла, содержащий словари с данными об этих файлах.
    """
    files_data = {}
    for filename in filenames:
        try:
            if not Path(filename).exists():
                logger.error(f"Файл '{filename}' не найден")
                continue

            file_hash = get_hash(filename)
            file_creation_date = get_modification_date(filename)

            files_data[filename] = dict(
                hash=file_hash,
                modify=file_creation_date,
                state='new',
            )
        except Exception as e:
            logger.error(f"Невозможно получить данные о файле '{filename}'", exc_info=e)
        else:
            file_data = json.dumps(files_data[filename])
            logger.debug(f"Получены данные о файле '{filename}':\n{file_data}")

    return files_data


def get_prev_files_data(folder: Union[str, Path]) -> Dict[str, Dict[str, str]]:
    """
    Возвращает словарь, где каждому имени файла соответствует хеш и дата редактирования.
    Если в папке не оказывается записей, то возвращается .

    Args:
        folder (str): путь к папке с записями о файлах.

    Returns:
        dict: словарь с ключём-именем файла, содержащий словари с данными об этих файлах.
        {}: пустой словарь в случае, если предыдущий файл с записями не существует или пуст.
    """
    last_info_filename = get_last_info_filename(folder)
    if last_info_filename is None:
        return {}

    try:
        with open(last_info_filename, 'rb') as f:
            prev_files_data = json.load(f)
        return prev_files_data

    except Exception as e:
        logger.error(f"Невозможно прочитать данные из '{last_info_filename}'", exc_info=e)
        return {}


def get_last_info_filename(folder: Union[str, Path]) -> Union[str, None]:
    """
    Возвращает последний актуальный файл с записями.

    Args:
        folder: папка, в которой хранятся записи.

    Returns:
        str: путь к последнему файлу с записями в папке.
        None: если записей нет.
    """
    folder = str(Path(folder))
    filenames = glob(f'{folder}/*.json')
    filenames.sort(reverse=True)

    if len(filenames) > 0:
        return filenames[0]
    return None


def mark_changed_files(
        prev_records: Dict[str, Any],
        curr_records: Dict[str, Any],
) -> None:
    """
    Записывает в curr_records состояния файлов (изменен, не изменён, создан)
    на основании данных из prev_records. Изменяет переданный curr_records!

    Args:
        prev_records (dict): словарь с предыдущими записями о файлах.
        curr_records (dict): словарь с актуальными записями о файлах.

    Returns:
        None: ничего не возвращает, т.к. изменяет curr_records, переданный в аргументах.
    """
    for filename in curr_records:
        if filename not in prev_records:
            curr_records[filename]['state'] = 'new'
            continue

        hash1 = curr_records[filename]['hash']
        hash2 = prev_records[filename]['hash']

        if hash1 != hash2:
            curr_records[filename]['state'] = 'changed'
        else:
            curr_records[filename]['state'] = 'unchanged'


def main() -> None:
    """
    Читает список файлов из json-конфига и получает их хеши и даты создания.
    Собранные данные сохраняются в новый файл.
    """
    try:
        filenames = get_file_list_from_json(CONFIG_FILENAME)
    except Exception as e:
        logger.critical(
            f"Ошибка во время чтения списка файлов из '{CONFIG_FILENAME}'. "
            f"Закрытие программы!",
            exc_info=e,
        )
        return

    curr_records = get_curr_files_data(filenames)
    prev_records = get_prev_files_data(SCRIPTS_INFO_FOLDER)

    mark_changed_files(prev_records, curr_records)

    date_str = datetime.now().strftime("%Y%m%dT%H%M%S")
    filename4dump = f'{SCRIPTS_INFO_FOLDER}/scripts_info_{date_str}.json'
    os.makedirs(Path(filename4dump).parent, exist_ok=True)

    try:
        with open(filename4dump, 'w') as f:
            json.dump(curr_records, f, sort_keys=True, indent=4)
    except Exception as e:
        logger.critical(
            f"Невозможно записать собранные данные в файл '{filename4dump}'",
            exc_info=e
        )
        return
    else:
        logger.debug(f"Собранные данные успешно записаны в файл '{filename4dump}'")


if __name__ == '__main__':

    # Путь к конфигу .json, в котором записаны все пути к файлам, которые нужно проверить
    try:
        CONFIG_FILENAME = sys.argv[1]
        loaded_config = get_config(CONFIG_FILENAME)
    except Exception as ex:
        print("Не удалось загрузить конфиг из-за ошибки:", ex)
        exit()

    # Путь к файлу, куда будут записаны логи
    # (лог автоматически ротируется, если достигает определённого размера)
    LOG_FILENAME = loaded_config['log_file']

    # Папка, куда будут записаны json'ки с хешами и другими данными файлов
    SCRIPTS_INFO_FOLDER = loaded_config['records_folder']

    log_filename = Path(LOG_FILENAME)
    os.makedirs(log_filename.parent, exist_ok=True)

    logger = setup_logger(log_filename, level=logging.DEBUG)

    try:
        main()
    except KeyboardInterrupt:
        logger.info("Выполнение прервано пользователем (Ctrl+C)")
    except BaseException as ex:
        logger.critical("Завершение из-за ошибки", exc_info=ex)
        raise
