import json
from pathlib import Path

from aiohttp import MultipartReader, BodyPartReader
from aiohttp.hdrs import CONTENT_TYPE


async def default_file_handler(file_name, content, save_to='~/media/'):
    path = Path(save_to)
    path.mkdir(parents=True, exist_ok=True)
    file_name = path.joinpath(file_name)
    # TODO: file type validation
    # TODO: slugify and sanitize file name
    with open(file_name, 'wb') as f:
        f.write(content)
    return file_name


async def multipart_parser(request, file_handler=default_file_handler):
    """
    :param file_handler: callable to save file, this should always return the file path
    :return: dictionary containing files and data
    """
    multipart_data = {
        'files': {},
        'data': {}
    }
    if request.content_type == 'multipart/form-data':
        reader = MultipartReader.from_response(request)

        while True:
            part = await reader.next()
            if part is None:
                break
            if isinstance(part, BodyPartReader):
                if part.filename:
                    # if body is binary file
                    if file_handler:
                        # in case we just want to parse data and not save file actually e.g. in validator
                        file_data = await part.read(decode=True)
                        file_data = part.decode(file_data)
                        file_path = await file_handler(part.filename, file_data, part.headers[CONTENT_TYPE])
                    else:
                        file_path = part.filename
                    multipart_data['files'][part.name] = file_path
                elif part.text():
                    # if body is text
                    text = await part.text()
                    multipart_data['data'][part.name] = text
                else:
                    # if body is json or form (not text), not handling this
                    continue
            else:
                # if part is recursive multipart , not handling this right now
                # TODO: do recursive call to handle this
                continue
    else:
        try:
            multipart_data['data'] = await request.json()
        except json.JSONDecodeError:
            pass
    return multipart_data
