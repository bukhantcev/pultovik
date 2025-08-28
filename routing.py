# routing.py
from aiogram import Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.filters import StateFilter
from handlers.start import cmd_start
from handlers.spectacles import handle_spectacles, spectacles_menu_router, edit_employees_start, edit_employees_toggle, edit_employees_done, add_spectacle_name, AddSpectacle, delete_spectacle, rename_spectacle_start, rename_spectacle_save, RenameSpectacle, edit_spectacle_start
from aiogram.fsm.context import FSMContext
from handlers.employees import handle_workers, employees_menu_router, emp_del_ask, emp_del_yes, emp_del_no, emp_tg_start, emp_tg_set_value, AddEmployee, EditEmployeeTg, add_employee_last_name, add_employee_first_name, add_employee_tg
from handlers.busy_user import busy_submit_text, busy_view_text, BusyInput, busy_submit, busy_view, handle_busy_add_text, handle_busy_remove_text, busy_add, busy_remove
from handlers.busy_admin import admin_busy_panel, emp_busy_view, emp_busy_add_start, emp_busy_remove_start, admin_handle_busy_add_text, admin_handle_busy_remove_text, AdminBusyInput
from handlers.excel import (
    import_schedule_start,
    handle_excel_upload,
    handle_excel_month_pick,
    handle_make_schedule,
    handle_make_schedule_pick,
    UploadExcel,
    unknown_toggle_employee,
    unknown_save_current,
    AssignUnknown,
    view_schedule_start,
    view_schedule_pick,
    publish_start,
    publish_month_pick,
)
from handlers.admin import handle_auto_assign, auth_list_employees, auth_approve, auth_deny, auth_new_start, auth_new_last_name, auth_new_first_name, NewAuthEmployee
from handlers.ai_fill import ai_fill_start, ai_fill_receive, ai_fill_cancel, AIFillStates, ai_fill_site_start, ai_fill_site_pick

def register(dp: Dispatcher):
    # base
    dp.message.register(cmd_start, CommandStart())

    # menus
    dp.message.register(handle_spectacles, F.text.lower() == "спектакли")
    dp.message.register(handle_workers, F.text.lower() == "сотрудники")

    dp.message.register(view_schedule_start, F.text == "Посмотреть расписание")
    dp.callback_query.register(view_schedule_pick, F.data.startswith('viewmonth:'))

    # admin actions
    dp.message.register(handle_auto_assign, F.text.regexp(r"(?i)^автоназначение"))
    dp.message.register(import_schedule_start, F.text.lower() == "импорт расписания")
    dp.message.register(handle_make_schedule, F.text.lower() == "сделать график")
    dp.message.register(publish_start, F.text == "Опубликовать")
    dp.message.register(admin_busy_panel, F.text.lower() == "busy_admin")

    dp.message.register(ai_fill_start, F.text.lower() == "ai заполнить шаблон")
    dp.callback_query.register(ai_fill_site_start, F.data == 'ai:site')
    dp.callback_query.register(ai_fill_site_pick,  F.data.startswith('ai:sitepick:'))

    # spectacles callbacks
    dp.callback_query.register(
        spectacles_menu_router,
        (F.data == 'add_spectacle') | F.data.startswith('title:') | F.data.startswith('t:')
    )
    dp.callback_query.register(edit_spectacle_start, F.data.startswith('edit_spectacle:'))
    dp.callback_query.register(edit_employees_start,  F.data.startswith('editstart:'))
    dp.callback_query.register(edit_employees_toggle, F.data.startswith('edittoggle:'))
    dp.callback_query.register(edit_employees_done,   F.data.startswith('editdone:'))
    dp.callback_query.register(delete_spectacle, F.data.startswith('del_spectacle:'))
    dp.callback_query.register(rename_spectacle_start, F.data.startswith('rename_spectacle:'))
    dp.message.register(rename_spectacle_save, StateFilter(RenameSpectacle.waiting_for_title))

    # employees callbacks
    dp.callback_query.register(employees_menu_router,  (F.data == 'emp:add') | F.data.startswith('emp:show:'))
    dp.callback_query.register(emp_del_yes,  F.data.startswith('emp:del:yes:'))
    dp.callback_query.register(emp_del_ask,  F.data.startswith('emp:del:ask:'))
    dp.callback_query.register(emp_del_no,   F.data == 'emp:del:no')
    dp.callback_query.register(emp_tg_start, F.data.startswith('emp:tg:start:'))

    # unknown spectacle handlers (after Excel registrations)
    dp.callback_query.register(unknown_toggle_employee, StateFilter(AssignUnknown.waiting), F.data.startswith('unkemp:'))
    dp.callback_query.register(unknown_save_current,    StateFilter(AssignUnknown.waiting), F.data == 'unksave')

    # admin auth callbacks
    dp.callback_query.register(auth_list_employees, F.data.startswith('auth:list:'))
    dp.callback_query.register(auth_approve,       F.data.startswith('auth:approve:'))
    dp.callback_query.register(auth_deny,          F.data.startswith('auth:deny:'))

    dp.callback_query.register(auth_new_start, F.data.startswith('auth:new:'))
    dp.message.register(auth_new_last_name, StateFilter(NewAuthEmployee.waiting_for_last_name))
    dp.message.register(auth_new_first_name, StateFilter(NewAuthEmployee.waiting_for_first_name))

    # FSM routes
    dp.message.register(add_spectacle_name, StateFilter(AddSpectacle.waiting_for_name))
    dp.message.register(emp_tg_set_value,   StateFilter(EditEmployeeTg.waiting_for_tg))
    dp.message.register(admin_handle_busy_add_text,    StateFilter(AdminBusyInput.waiting_for_add))
    dp.message.register(admin_handle_busy_remove_text, StateFilter(AdminBusyInput.waiting_for_remove))

    dp.message.register(add_employee_last_name, StateFilter(AddEmployee.waiting_for_last_name))
    dp.message.register(add_employee_first_name, StateFilter(AddEmployee.waiting_for_first_name))
    dp.message.register(add_employee_tg,        StateFilter(AddEmployee.waiting_for_tg_id))

    # busy (user)
    dp.callback_query.register(busy_submit, F.data == 'busy:submit')
    dp.callback_query.register(busy_view,   F.data == 'busy:view')
    dp.callback_query.register(busy_add,    F.data == 'busy:add')
    dp.callback_query.register(busy_remove, F.data == 'busy:remove')
    dp.message.register(busy_submit_text, F.text.regexp(r"^Подать даты за "))
    dp.message.register(busy_view_text,   F.text.lower() == "посмотреть свои даты")
    dp.message.register(handle_busy_add_text,    StateFilter(BusyInput.waiting_for_add_user))
    dp.message.register(handle_busy_remove_text, StateFilter(BusyInput.waiting_for_remove_user))

    # admin busy per-employee
    dp.callback_query.register(emp_busy_view,         F.data.startswith('emp:busy:view:'))
    dp.callback_query.register(emp_busy_add_start,    F.data.startswith('empbusy:add:'))
    dp.callback_query.register(emp_busy_remove_start, F.data.startswith('empbusy:remove:'))

    # excel (scoped to FSM states to avoid collisions)
    dp.message.register(
        handle_excel_upload,
        StateFilter(UploadExcel.waiting_for_file),
        (F.document | F.photo)
    )
    dp.callback_query.register(handle_excel_month_pick, StateFilter(UploadExcel.waiting_for_month), F.data.startswith('xlsmonth:'))
    dp.callback_query.register(handle_make_schedule_pick, F.data.startswith('mkmonth:'))
    dp.callback_query.register(publish_month_pick, F.data.startswith('pubmonth:'))

    # AI fill FSM
    dp.message.register(ai_fill_cancel,  StateFilter(AIFillStates.waiting_for_file), F.text.lower() == "отмена")
    dp.message.register(ai_fill_receive, StateFilter(AIFillStates.waiting_for_file), (F.document | F.photo))

    # Ensure only one registration for ai_fill_start (keep the one near the top with other menu commands)