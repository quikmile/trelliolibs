import yagmail

def send(receivers,subject, content):
    return yagmail.SMTP('artificilabstest@gmail.com').send(receivers, subject, content)
