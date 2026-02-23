# This file is included by DuckDB's build system. It specifies which extension to load

# Always build cql2elm GraalVM native image and embed it (no skip option)

# cql2elm native image build configuration
string(TOLOWER "${CMAKE_SYSTEM_NAME}" CQL2ELM_SYS_LOWER)
set(CQL2ELM_ARCH "${CMAKE_SYSTEM_PROCESSOR}")
set(CQL2ELM_BE_DIR "${CMAKE_CURRENT_LIST_DIR}/cql2elm-be")
set(GRAAL_CONF_DIR "${CMAKE_CURRENT_LIST_DIR}/graalvm-config")
set(CQL2ELM_NATIVE_DIR "${CQL2ELM_BE_DIR}/native-libs/${CQL2ELM_SYS_LOWER}-${CQL2ELM_ARCH}")
set(CQL2ELM_NATIVE_SO "${CQL2ELM_NATIVE_DIR}/libcql2elm-native.so")
set(CQL2ELM_NATIVE_BUILD_STAMP "${CQL2ELM_BE_DIR}/native-libs/BUILD_INFO.txt")

find_program(MAVEN_CMD mvn REQUIRED)
find_program(NATIVE_IMAGE_CMD native-image REQUIRED)
find_program(BASH_CMD bash REQUIRED)
find_program(XXD_EXECUTABLE xxd)

# Build script (generated at configure time)
set(CQL2ELM_BUILD_SCRIPT "${CMAKE_CURRENT_BINARY_DIR}/build_cql2elm_native.sh")
file(WRITE  ${CQL2ELM_BUILD_SCRIPT} "#!/usr/bin/env bash\n")
file(APPEND ${CQL2ELM_BUILD_SCRIPT} "set -euo pipefail\n")
file(APPEND ${CQL2ELM_BUILD_SCRIPT} "echo '[cql2elm-native] Maven build'\n")
file(APPEND ${CQL2ELM_BUILD_SCRIPT} "mvn -q -DskipTests -Dmaven.test.skip=true -Djacoco.skip=true -Dskip.unit.tests=true clean package dependency:build-classpath -DincludeScope=runtime -Dmdep.outputFile=target/classpath.txt\n")
file(APPEND ${CQL2ELM_BUILD_SCRIPT} "cp target/cql2elm-native-*.jar target/cql2elm-native.jar\n")
file(APPEND ${CQL2ELM_BUILD_SCRIPT} "mkdir -p target/bootstrap\n")
file(APPEND ${CQL2ELM_BUILD_SCRIPT} "cat > target/bootstrap/Cql2ElmBootstrapMain.java <<'JAV'\npublic class Cql2ElmBootstrapMain { public static void main(String[] a){ System.out.println(\"cql2elm native image bootstrap\"); } }\nJAV\n")
file(APPEND ${CQL2ELM_BUILD_SCRIPT} "javac -cp target/cql2elm-native.jar -d target/bootstrap target/bootstrap/Cql2ElmBootstrapMain.java\n")
file(APPEND ${CQL2ELM_BUILD_SCRIPT} "CP=$(tr -d '\r' < target/classpath.txt)\n")
file(APPEND ${CQL2ELM_BUILD_SCRIPT} "echo '[cql2elm-native] native-image'\n")
file(APPEND ${CQL2ELM_BUILD_SCRIPT} "${NATIVE_IMAGE_CMD} --no-fallback --shared --enable-all-security-services -H:+ReportExceptionStackTraces -J-Xss8m -J-Xmx4g -R:StackSize=16m -H:ConfigurationFileDirectories='${GRAAL_CONF_DIR}' -H:Name=cql2elm-native -cp target/cql2elm-native.jar:target/bootstrap:$CP -H:Class=Cql2ElmBootstrapMain\n")
file(APPEND ${CQL2ELM_BUILD_SCRIPT} "mkdir -p '${CQL2ELM_NATIVE_DIR}'\n")
file(APPEND ${CQL2ELM_BUILD_SCRIPT} "# Handle both Linux (.so) and macOS (.dylib) outputs\n")
file(APPEND ${CQL2ELM_BUILD_SCRIPT} "if [ -f cql2elm-native.dylib ]; then\n")
file(APPEND ${CQL2ELM_BUILD_SCRIPT} "  mv -f cql2elm-native.dylib '${CQL2ELM_NATIVE_SO}'\n")
file(APPEND ${CQL2ELM_BUILD_SCRIPT} "elif [ -f cql2elm-native.so ]; then\n")
file(APPEND ${CQL2ELM_BUILD_SCRIPT} "  mv -f cql2elm-native.so '${CQL2ELM_NATIVE_SO}'\n")
file(APPEND ${CQL2ELM_BUILD_SCRIPT} "else\n")
file(APPEND ${CQL2ELM_BUILD_SCRIPT} "  echo 'Error: Neither cql2elm-native.dylib nor cql2elm-native.so found!' >&2\n")
file(APPEND ${CQL2ELM_BUILD_SCRIPT} "  exit 1\n")
file(APPEND ${CQL2ELM_BUILD_SCRIPT} "fi\n")
file(APPEND ${CQL2ELM_BUILD_SCRIPT} "cp -f target/cql2elm-native.jar '${CQL2ELM_NATIVE_DIR}/'\n")
file(APPEND ${CQL2ELM_BUILD_SCRIPT} "{ echo 'Build Timestamp: '$(date -u +'%Y-%m-%dT%H:%M:%SZ'); echo 'Git Commit: '$(git rev-parse --short HEAD 2>/dev/null || echo unknown); echo 'OS: ${CQL2ELM_SYS_LOWER}'; echo 'Arch: ${CQL2ELM_ARCH}'; echo 'Jar: cql2elm-native.jar'; echo 'Native Library: libcql2elm-native.so'; } > native-libs/BUILD_INFO.txt\n")
file(APPEND ${CQL2ELM_BUILD_SCRIPT} "echo '[cql2elm-native] Done (native lib: ${CQL2ELM_NATIVE_SO})'\n")
file(APPEND ${CQL2ELM_BUILD_SCRIPT} "exit 0\n")
execute_process(COMMAND chmod 0755 ${CQL2ELM_BUILD_SCRIPT})

add_custom_command(
  OUTPUT ${CQL2ELM_NATIVE_SO} ${CQL2ELM_NATIVE_BUILD_STAMP}
  COMMAND ${CMAKE_COMMAND} -E make_directory ${GRAAL_CONF_DIR}
  COMMAND ${CMAKE_COMMAND} -E make_directory ${CQL2ELM_NATIVE_DIR}
  COMMAND ${BASH_CMD} ${CQL2ELM_BUILD_SCRIPT}
  DEPENDS ${CMAKE_CURRENT_LIST_DIR}/graalvm-config/reflect-config.json ${CMAKE_CURRENT_LIST_DIR}/graalvm-config/resource-config.json
  WORKING_DIRECTORY ${CQL2ELM_BE_DIR}
  COMMENT "Building cql2elm native shared library via script (GraalVM native-image)"
  VERBATIM
)
add_custom_target(cql2elm_native ALL DEPENDS ${CQL2ELM_NATIVE_SO} ${CQL2ELM_NATIVE_BUILD_STAMP})

# Embed native library into header (if xxd available)
if(XXD_EXECUTABLE)
  set(CQL2ELM_EMBED_HEADER "${CMAKE_CURRENT_BINARY_DIR}/cql2elm_native_embedded.h")
  add_custom_command(
    OUTPUT ${CQL2ELM_EMBED_HEADER}
    COMMAND ${XXD_EXECUTABLE} -i -n cql2elm_native_blob ${CQL2ELM_NATIVE_SO} > ${CQL2ELM_EMBED_HEADER}.tmp
    COMMAND ${CMAKE_COMMAND} -E rename ${CQL2ELM_EMBED_HEADER}.tmp ${CQL2ELM_EMBED_HEADER}
    DEPENDS cql2elm_native ${CQL2ELM_NATIVE_SO}
    COMMENT "Embedding cql2elm native library (cql2elm_native_embedded.h)"
    VERBATIM
  )
  add_custom_target(cql2elm_embed_header ALL DEPENDS ${CQL2ELM_EMBED_HEADER})
else()
  message(WARNING "xxd not found; cql2elm native library will not be embedded")
endif()

add_compile_definitions(CQL2ELM_EMBEDDED_NATIVE_LIB)

# Post-target configuration (deferred so targets exist)
function(configure_cql2elm_extension_dependencies)
  set(target_name cql2elm)
  if(TARGET ${target_name})
    target_include_directories(${target_name} PRIVATE ${CMAKE_CURRENT_LIST_DIR}/src/include/cql2elm_native)
    target_include_directories(${target_name} PRIVATE ${CMAKE_CURRENT_BINARY_DIR})
    target_compile_definitions(${target_name} PRIVATE CQL2ELM_EMBEDDED_NATIVE_LIB)
    if(TARGET cql2elm_native)
      add_dependencies(${target_name} cql2elm_native)
    endif()
    if(TARGET cql2elm_embed_header)
      add_dependencies(${target_name} cql2elm_embed_header)
    endif()
  endif()
endfunction()
cmake_language(DEFER CALL configure_cql2elm_extension_dependencies)
