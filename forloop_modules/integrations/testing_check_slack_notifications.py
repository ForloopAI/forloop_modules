import datetime

from keepvariable.keepvariable_core import save_variables, load_variable_safe

import forloop_modules.flog as flog

from forloop_modules.integrations.slack_integration import send_message_to_slack_direct_execute


from config.config import other_config #TODO Dominik: Circular dependency to forloop_platform repository # not ideal #Maybe solve with os.environ?
slack_token = other_config.SLACK_TOKEN

SLACK_NOTIFICATION_INTERVAL=1#3600*24 #by default should be 1s, if you are extensively testing the tests, you can increase it temporarily


def get_login():
    try:
        login_email = load_variable_safe(filename='vars.kpv', varname="login_email")
    except:
        flog.warning(f"Unknown developer login error")
        login_email = 'Unknown developer'

    return login_email

login_email=get_login()


def send_notification_to_slack_no_spam(msg, filename):
    send_message_to_slack = False

    try:
        last_notification_datetime = load_variable_safe(filename=filename, varname="last_notification_datetime")
        last_notification_datetime = datetime.datetime.fromisoformat(last_notification_datetime)
    except(FileNotFoundError, KeyError):
        send_message_to_slack = True

    if not send_message_to_slack:
        if (datetime.datetime.now() - last_notification_datetime).total_seconds() > SLACK_NOTIFICATION_INTERVAL:
            send_message_to_slack = True

    if send_message_to_slack:

        send_message_to_slack_direct_execute(slack_token, "testing", msg)
        last_notification_datetime = str(datetime.datetime.now().replace(microsecond=0))
        save_variables({'last_notification_datetime': last_notification_datetime}, filename=filename)

def inform_about_testing(test_result):

    last_test_datetime = str(datetime.datetime.now().replace(microsecond=0))
    save_variables({'last_test_datetime': last_test_datetime}, filename='last_test.kpv')

    message_to_slack=f'{login_email} just tested the platform.\nTime of the test: {last_test_datetime}\nResult: {test_result}'
    send_notification_to_slack_no_spam(message_to_slack, 'last_test_notification.kpv')

def warn_if_last_test_too_long_ago():

    try:
        last_test_datetime = load_variable_safe(filename='last_test.kpv', varname="last_test_datetime")
    except:
        flog.flog('No record about testing the platform. Please test it.', '', flog.LogColor.ERROR)

        send_notification_to_slack_no_spam(f"Developer {login_email} has no record about testing the platform. He should test it.", 'last_warning_notification.kpv')
        return

    last_test_datetime = datetime.datetime.fromisoformat(last_test_datetime)

    current_datetime = datetime.datetime.now().replace(microsecond=0)
    # last_test_datetime = last_test_datetime.replace(year=2020)
    time_from_last_test = (current_datetime - last_test_datetime).total_seconds()

    if time_from_last_test <= 86400:  # 1 day
        flog.flog('The platform was tested recently.', '', flog.LogColor.OKGREEN)

    elif time_from_last_test > 86400 and time_from_last_test <= 86400 * 10:
        flog.warning('The platform was not tested in the last day. Please consider testing it.')
    else:
        flog.flog('The platform was not tested in last 10 days. Please test it.', '', flog.LogColor.ERROR)
        message_to_slack = f"Developer {login_email} is launching the platform but has not tested it in last 10 days."
        send_notification_to_slack_no_spam(message_to_slack, 'last_warning_notification.kpv')

