# From rustyconover/arrow-nanoarrow commit d83a538b1c0442d9fe4f5ecf697ce681893bed2e.
# This fix and the writer metadata API currently live on separate fork branches.
find_package(Git REQUIRED)
execute_process(COMMAND "${GIT_EXECUTABLE}" apply --reverse --check "${VGI_PATCH}"
    RESULT_VARIABLE already_applied OUTPUT_QUIET ERROR_QUIET)
if(NOT already_applied EQUAL 0)
    execute_process(COMMAND "${GIT_EXECUTABLE}" apply "${VGI_PATCH}"
        RESULT_VARIABLE applied ERROR_VARIABLE patch_error)
    if(NOT applied EQUAL 0)
        message(FATAL_ERROR "Could not apply nanoarrow's metadata NUL fix: ${patch_error}")
    endif()
endif()
