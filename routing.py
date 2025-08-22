# routing.py
from aiogram import Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.filters import StateFilter
from handlers.start import cmd_start
from handlers.spectacles import handle_spectacles, spectacles_menu_router, edit_employees_start, edit_employees_toggle, edit_employees_done, add_spectacle_name, AddSpectacle, delete_spectacle
from aiogram.fsm.context import FSMContext
from handlers.employees import handle_workers, employees_menu_router, emp_del_ask, emp_del_yes, emp_del_no, emp_tg_start, emp_tg_set_value, AddEmployee, EditEmployeeTg
from handlers.busy_user import busy_submit_text, busy_view_text, BusyInput, busy_submit, busy_view, handle_busy_add_text, handle_busy_remove_text
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
)
from handlers.admin import handle_auto_assign
from handlers.ai_fill import ai_fill_start, ai_fill_receive, ai_fill_cancel, AIFillStates

def register(dp: Dispatcher):
    # base
    dp.message.register(cmd_start, CommandStart())

    # menus
    dp.message.register(handle_spectacles, F.text.lower() == "спектакли")
    dp.message.register(handle_workers, F.text.lower() == "сотрудники")

    # admin actions
    dp.message.register(handle_auto_assign, F.text.regexp(r"(?i)^автоназначение"))
    dp.message.register(import_schedule_start, F.text.lower() == "импорт расписания")
    dp.message.register(handle_make_schedule, F.text.lower() == "сделать график")
    dp.message.register(admin_busy_panel, F.text.lower() == "busy_admin")

    dp.message.register(ai_fill_start, F.text.lower() == "ai заполнить шаблон")

    # spectacles callbacks
    dp.callback_query.register(
        spectacles_menu_router,
        (F.data == 'add_spectacle') | F.data.startswith('title:') | F.data.startswith('t:')
    )
    dp.callback_query.register(edit_employees_start,  F.data.startswith('editstart:'))
    dp.callback_query.register(edit_employees_toggle, F.data.startswith('edittoggle:'))
    dp.callback_query.register(edit_employees_done,   F.data.startswith('editdone:'))
    dp.callback_query.register(delete_spectacle, F.data.startswith('del_spectacle:'))

    # employees callbacks
    dp.callback_query.register(employees_menu_router,  (F.data == 'emp:add') | F.data.startswith('emp:show:'))
    dp.callback_query.register(emp_del_yes,  F.data.startswith('emp:del:yes:'))
    dp.callback_query.register(emp_del_ask,  F.data.startswith('emp:del:ask:'))
    dp.callback_query.register(emp_del_no,   F.data == 'emp:del:no')
    dp.callback_query.register(emp_tg_start, F.data.startswith('emp:tg:start:'))

    # unknown spectacle handlers (after Excel registrations)
    dp.callback_query.register(unknown_toggle_employee, StateFilter(AssignUnknown.waiting), F.data.startswith('unkemp:'))
    dp.callback_query.register(unknown_save_current,    StateFilter(AssignUnknown.waiting), F.data == 'unksave')

    # FSM routes
    dp.message.register(add_spectacle_name, StateFilter(AddSpectacle.waiting_for_name))
    dp.message.register(emp_tg_set_value,   StateFilter(EditEmployeeTg.waiting_for_tg))
    dp.message.register(admin_handle_busy_add_text,    StateFilter(AdminBusyInput.waiting_for_add))
    dp.message.register(admin_handle_busy_remove_text, StateFilter(AdminBusyInput.waiting_for_remove))

    # busy (user)
    dp.callback_query.register(busy_submit, F.data == 'busy:submit')
    dp.callback_query.register(busy_view,   F.data == 'busy:view')
    dp.message.register(busy_submit_text, F.text.regexp(r"^Подать даты за "))
    dp.message.register(busy_view_text,   F.text.lower() == "посмотреть свои даты")
    dp.message.register(handle_busy_add_text,    StateFilter(BusyInput.waiting_for_add_user))
    dp.message.register(handle_busy_remove_text, StateFilter(BusyInput.waiting_for_remove_user))

    # admin busy per-employee
    dp.callback_query.register(emp_busy_view,         F.data.startswith('emp:busy:view:'))
    dp.callback_query.register(emp_busy_add_start,    F.data.startswith('empbusy:add:'))
    dp.callback_query.register(emp_busy_remove_start, F.data.startswith('empbusy:remove:'))

    # excel (scoped to FSM states to avoid collisions)
    dp.message.register(handle_excel_upload, StateFilter(UploadExcel.waiting_for_file), F.document)
    dp.callback_query.register(handle_excel_month_pick, StateFilter(UploadExcel.waiting_for_month), F.data.startswith('xlsmonth:'))
    dp.callback_query.register(handle_make_schedule_pick, F.data.startswith('mkmonth:'))

    # AI fill FSM
    dp.message.register(ai_fill_cancel,  StateFilter(AIFillStates.waiting_for_file), F.text.lower() == "отмена")
    dp.message.register(ai_fill_receive, StateFilter(AIFillStates.waiting_for_file), (F.document | F.photo))

    # Ensure only one registration for ai_fill_start (keep the one near the top with other menu commands)