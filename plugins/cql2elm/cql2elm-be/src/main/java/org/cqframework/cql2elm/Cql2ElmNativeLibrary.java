package org.cqframework.cql2elm;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.cqframework.cql.cql2elm.CqlCompilerException;
import org.cqframework.cql.cql2elm.CqlCompilerOptions;
import org.cqframework.cql.cql2elm.CqlTranslator;
import org.cqframework.cql.cql2elm.LibraryManager;
import org.cqframework.cql.cql2elm.ModelManager;
import org.cqframework.cql.cql2elm.ModelInfoLoader;
import org.cqframework.cql.cql2elm.LibrarySourceProvider;
import org.cqframework.cql.elm.serializing.jackson.ElmJsonLibraryWriter;
import org.hl7.cql.model.ModelIdentifier;
import org.hl7.cql.model.ModelInfoProvider;
import org.hl7.elm.r1.Library;
import org.hl7.elm_modelinfo.r1.ModelInfo;
import org.hl7.elm_modelinfo.r1.serializing.jackson.XmlModelInfoReader;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;

import com.fasterxml.jackson.databind.ObjectMapper;

/** GraalVM native-image entry point for CQL-to-ELM translation. Bypasses ServiceLoader. */
public class Cql2ElmNativeLibrary {

    /** Read model info XML directly, bypassing ModelInfoReaderFactory ServiceLoader. */
    private static ModelInfo readModelInfoXml(String resourcePath) {
        try {
            // Try multiple classloader strategies for native-image compatibility
            InputStream is = Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream(resourcePath.startsWith("/") ? resourcePath.substring(1) : resourcePath);
            if (is == null) {
                is = Cql2ElmNativeLibrary.class.getResourceAsStream(resourcePath);
            }
            if (is == null) {
                return null;
            }
            try {
                return new XmlModelInfoReader().read(is);
            } finally {
                is.close();
            }
        } catch (Exception e) {
            return null;
        }
    }

    private static ModelInfoProvider createSystemProvider() {
        return new ModelInfoProvider() {
            @Override
            public ModelInfo load(ModelIdentifier id) {
                if (id == null || !"System".equals(id.getId())) return null;
                return readModelInfoXml("/org/hl7/elm/r1/system-modelinfo.xml");
            }
        };
    }

    private static ModelInfoProvider createFhirProvider() {
        final Map<String, String> versions = new HashMap<>();
        versions.put("4.0.1", "org/hl7/fhir/fhir-modelinfo-4.0.1.xml");
        versions.put("4.0.0", "org/hl7/fhir/fhir-modelinfo-4.0.0.xml");
        versions.put("3.2.0", "org/hl7/fhir/fhir-modelinfo-3.2.0.xml");
        versions.put("3.0.1", "org/hl7/fhir/fhir-modelinfo-3.0.1.xml");
        versions.put("3.0.0", "org/hl7/fhir/fhir-modelinfo-3.0.0.xml");
        versions.put("1.8", "org/hl7/fhir/fhir-modelinfo-1.8.xml");
        versions.put("1.6", "org/hl7/fhir/fhir-modelinfo-1.6.xml");
        versions.put("1.4", "org/hl7/fhir/fhir-modelinfo-1.4.xml");
        versions.put("1.0.2", "org/hl7/fhir/fhir-modelinfo-1.0.2.xml");
        return new ModelInfoProvider() {
            @Override
            public ModelInfo load(ModelIdentifier id) {
                if (id == null || !"FHIR".equals(id.getId())) return null;
                String version = id.getVersion();
                String path = version != null ? versions.get(version) : "org/hl7/fhir/fhir-modelinfo-4.0.1.xml";
                if (path == null) return null;
                return readModelInfoXml("/" + path);
            }
        };
    }

    private static final Pattern USING_FHIR_PATTERN =
            Pattern.compile("using\\s+FHIR\\s+version\\s+'([^']+)'");

    /** Explicitly inject FHIRHelpers include so the compiler resolves it via LibraryManager. */
    private static String ensureFhirHelpersInclude(String cqlText) {
        if (!cqlText.contains("include FHIRHelpers")) {
            Matcher m = USING_FHIR_PATTERN.matcher(cqlText);
            if (m.find()) {
                String fhirVersion = m.group(1);
                String include = " include FHIRHelpers version '" + fhirVersion + "'";
                cqlText = cqlText.substring(0, m.end()) + include + cqlText.substring(m.end());
            }
        }
        return cqlText;
    }

    /** FHIR library source provider using context classloader for GraalVM compatibility. */
    private static LibrarySourceProvider createFhirLibrarySourceProvider() {
        return new LibrarySourceProvider() {
            @Override
            public InputStream getLibrarySource(org.hl7.elm.r1.VersionedIdentifier id) {
                if (id == null || id.getId() == null) return null;
                String version = id.getVersion();
                if (version == null || version.isEmpty()) return null;
                String resourcePath = String.format("org/hl7/fhir/%s-%s.cql", id.getId(), version);
                InputStream is = Thread.currentThread().getContextClassLoader()
                        .getResourceAsStream(resourcePath);
                if (is == null) {
                    is = Cql2ElmNativeLibrary.class.getResourceAsStream("/" + resourcePath);
                }
                return is;
            }
        };
    }

    @CEntryPoint(name = "cql2elm_translate")
    public static CCharPointer translate(IsolateThread thread, CCharPointer cqlTextPtr) {
        try {
            String cqlText = CTypeConversion.toJavaString(cqlTextPtr);
            cqlText = ensureFhirHelpersInclude(cqlText);

            ModelManager modelManager = new ModelManager();
            ModelInfoLoader modelInfoLoader = modelManager.getModelInfoLoader();
            modelInfoLoader.registerModelInfoProvider(createSystemProvider());
            modelInfoLoader.registerModelInfoProvider(createFhirProvider());

            CqlCompilerOptions options = CqlCompilerOptions.defaultOptions();

            LibraryManager libraryManager = new LibraryManager(modelManager, options);
            libraryManager.getLibrarySourceLoader().registerProvider(createFhirLibrarySourceProvider());

            CqlTranslator translator = CqlTranslator.fromText(cqlText, libraryManager);

            List<CqlCompilerException> errors = translator.getErrors();
            if (errors != null && !errors.isEmpty()) {
                List<String> messages = new ArrayList<>();
                for (CqlCompilerException error : errors) {
                    messages.add(error.getMessage());
                }
                String errorJson = new ObjectMapper().writeValueAsString(
                    new ErrorResult(messages)
                );
                return CTypeConversion.toCString(errorJson).get();
            }

            // Write ELM directly to bypass ElmLibraryWriterFactory ServiceLoader.
            Library elm = translator.toELM();
            ElmJsonLibraryWriter writer = new ElmJsonLibraryWriter();
            String elmJson = writer.writeAsString(elm);
            return CTypeConversion.toCString(elmJson).get();

        } catch (Exception e) {
            try {
                List<String> messages = new ArrayList<>();
                messages.add(e.getClass().getName() + ": " + e.getMessage());
                String errorJson = new ObjectMapper().writeValueAsString(
                    new ErrorResult(messages)
                );
                return CTypeConversion.toCString(errorJson).get();
            } catch (Exception e2) {
                return CTypeConversion.toCString("{\"error\":true,\"messages\":[\"Internal error\"]}").get();
            }
        }
    }

    public static class ErrorResult {
        public boolean error = true;
        public List<String> messages;

        public ErrorResult(List<String> messages) {
            this.messages = messages;
        }
    }

    public static void main(String[] args) {
        System.out.println("Cql2ElmNativeLibrary ready");
    }
}
