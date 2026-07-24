/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.jni;

import scala.runtime.BoxedUnit;

import org.apache.gluten.exception.GlutenException;
import org.apache.spark.util.SparkShutdownManagerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.InvalidPathException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.Vector;

public class JniLibLoader {
    private static final Logger LOG = LoggerFactory.getLogger(JniLibLoader.class);

    private static final Set<String> LOADED_LIBRARY_PATHS =
            Collections.synchronizedSet(new HashSet<>());
    private static final Set<String> REQUIRE_UNLOAD_LIBRARY_PATHS =
            Collections.synchronizedSet(new LinkedHashSet<>());

    static {
        SparkShutdownManagerUtil.addHookForLibUnloading(
                () -> {
                    forceUnloadAll();
                    return BoxedUnit.UNIT;
                });
    }

    private final String workDir;
    private final Set<String> loadedLibraries = Collections.synchronizedSet(new HashSet<>());

    JniLibLoader(String workDir) {
        this.workDir = workDir;
    }

    /**
     * Force unloads all libraries that were loaded with unload tracking enabled.
     */
    public static void forceUnloadAll() {
        List<String> loaded;
        synchronized (REQUIRE_UNLOAD_LIBRARY_PATHS) {
            loaded = new ArrayList<>(REQUIRE_UNLOAD_LIBRARY_PATHS);
        }
        Collections.reverse(loaded);
        loaded.forEach(JniLibLoader::unloadFromPath);
    }

    private static String toRealPath(String libPath) {
        Path currentPath = Paths.get(libPath);
        try {
            while (Files.isSymbolicLink(currentPath)) {
                Path linkTarget = Files.readSymbolicLink(currentPath);
                currentPath = linkTarget.isAbsolute()
                        ? linkTarget
                        : currentPath.getParent().resolve(linkTarget).normalize();
            }
            String realPath = currentPath.toString();
            LOG.info("Read real path {} for libPath {}", realPath, libPath);
            return realPath;
        } catch (IOException | InvalidPathException e) {
            throw new GlutenException("Error to read real path for libPath: " + libPath, e);
        }
    }

    private static void loadFromPath0(String libPath, boolean shouldUnload) {
        String realPath = toRealPath(libPath);
        synchronized (LOADED_LIBRARY_PATHS) {
            if (LOADED_LIBRARY_PATHS.contains(realPath)) {
                LOG.debug("Library in path {} has already been loaded, skipping", realPath);
            } else {
                System.load(realPath);
                LOADED_LIBRARY_PATHS.add(realPath);
                LOG.info("Library {} has been loaded using path-loading method", realPath);
            }
        }
        if (shouldUnload) {
            synchronized (REQUIRE_UNLOAD_LIBRARY_PATHS) {
                REQUIRE_UNLOAD_LIBRARY_PATHS.add(realPath);
            }
        }
    }

    /**
     * Loads a native library from the given absolute path.
     *
     * @param libPath path to the native library file
     * @param shouldUnload whether the library should be tracked for shutdown-time unloading
     */
    public static synchronized void loadFromPath(String libPath, boolean shouldUnload) {
        File file = new File(libPath);
        if (!file.exists() || !file.isFile()) {
            throw new GlutenException(
                    "library at path: " + libPath + " is not a file or does not exist");
        }
        loadFromPath0(file.getAbsolutePath(), shouldUnload);
    }

    /**
     * Unloads a native library from the given path when it was previously loaded.
     *
     * @param libPath path to the native library file
     */
    public static void unloadFromPath(String libPath) {
        String realPath = toRealPath(libPath);
        if (!removeLoadedLibrary(realPath)) {
            LOG.warn("Library {} was not loaded or already unloaded:", realPath);
            return;
        }
        LOG.info("Starting unload library path: {}", realPath);
        synchronized (REQUIRE_UNLOAD_LIBRARY_PATHS) {
            REQUIRE_UNLOAD_LIBRARY_PATHS.remove(realPath);
        }
        try {
            Vector<Object> nativeLibraries = getNativeLibraries();
            finalizeLibrary(nativeLibraries, realPath);
        } catch (ReflectiveOperationException | SecurityException e) {
            LOG.error("Unload native library error: ", e);
        }
    }

    /**
     * Loads a library resource into the work directory and optionally tracks it for unloading.
     *
     * @param libPath library resource path
     * @param shouldUnload whether the library should be tracked for shutdown-time unloading
     */
    public void load(String libPath, boolean shouldUnload) {
        synchronized (loadedLibraries) {
            try {
                if (loadedLibraries.contains(libPath)) {
                    LOG.debug("Library {} has already been loaded, skipping", libPath);
                    return;
                }
                File file = moveToWorkDir(workDir, libPath);
                loadWithLink(file.getAbsolutePath(), null, shouldUnload);
                loadedLibraries.add(libPath);
                LOG.info("Successfully loaded library {}", libPath);
            } catch (IOException e) {
                throw new GlutenException(e);
            }
        }
    }

    /**
     * Loads a library resource into the work directory and creates a symbolic link for it.
     *
     * @param libPath library resource path
     * @param linkName symbolic link name to create in the work directory
     * @param shouldUnload whether the library should be tracked for shutdown-time unloading
     */
    public void loadAndCreateLink(String libPath, String linkName, boolean shouldUnload) {
        synchronized (loadedLibraries) {
            try {
                if (loadedLibraries.contains(libPath)) {
                    LOG.debug("Library {} has already been loaded, skipping", libPath);
                    return;
                }
                File file = moveToWorkDir(workDir, libPath);
                loadWithLink(file.getAbsolutePath(), linkName, shouldUnload);
                loadedLibraries.add(libPath);
                LOG.info("Successfully loaded library {}", libPath);
            } catch (IOException e) {
                throw new GlutenException(e);
            }
        }
    }

    private static boolean removeLoadedLibrary(String libPath) {
        synchronized (LOADED_LIBRARY_PATHS) {
            return LOADED_LIBRARY_PATHS.remove(libPath);
        }
    }

    @SuppressWarnings("unchecked")
    private static Vector<Object> getNativeLibraries()
            throws NoSuchFieldException, IllegalAccessException {
        ClassLoader classLoader = JniLibLoader.class.getClassLoader();
        Field field = ClassLoader.class.getDeclaredField("nativeLibraries");
        field.setAccessible(true);
        return (Vector<Object>) field.get(classLoader);
    }

    private static void finalizeLibrary(Vector<Object> nativeLibraries, String libPath)
            throws ReflectiveOperationException {
        String targetFileName = new File(libPath).getName();
        synchronized (nativeLibraries) {
            Iterator<Object> iterator = nativeLibraries.iterator();
            while (iterator.hasNext()) {
                Object library = iterator.next();
                finalizeLibraryIfMatched(library, targetFileName);
            }
        }
    }

    private static void finalizeLibraryIfMatched(Object library, String targetFileName)
            throws ReflectiveOperationException {
        Optional<Field> libraryNameField = findLibraryNameField(library);
        if (!libraryNameField.isPresent()) {
            return;
        }
        Field nameField = libraryNameField.get();
        nameField.setAccessible(true);
        String verbosePath = String.valueOf(nameField.get(library));
        String verboseFileName = new File(verbosePath).getName();
        if (!targetFileName.equals(verboseFileName)) {
            return;
        }
        LOG.info("Finalizing library file: {}", targetFileName);
        Method finalizeMethod = library.getClass().getDeclaredMethod("finalize");
        finalizeMethod.setAccessible(true);
        finalizeMethod.invoke(library);
    }

    private static Optional<Field> findLibraryNameField(Object library) {
        for (Field field : library.getClass().getDeclaredFields()) {
            if ("name".equals(field.getName())) {
                return Optional.of(field);
            }
        }
        return Optional.empty();
    }

    private File moveToWorkDir(String workDir, String libraryToLoad) throws IOException {
        Path libPath = Paths.get(workDir, libraryToLoad);
        if (Files.exists(libPath)) {
            Files.delete(libPath);
        }
        Path parentPath = libPath.getParent();
        if (parentPath != null) {
            Files.createDirectories(parentPath);
        }
        try (InputStream inputStream =
                JniLibLoader.class.getClassLoader().getResourceAsStream(libraryToLoad)) {
            if (inputStream == null) {
                throw new FileNotFoundException(libraryToLoad);
            }
            Files.copy(inputStream, libPath);
        }
        return libPath.toFile();
    }

    private void loadWithLink(String libPath, String linkName, boolean shouldUnload)
            throws IOException {
        loadFromPath0(libPath, shouldUnload);
        LOG.info("Library {} has been loaded", libPath);
        if (linkName == null) {
            return;
        }
        Path target = Paths.get(libPath);
        Path link = Paths.get(workDir, linkName);
        if (Files.exists(link)) {
            LOG.info("Symbolic link already exists for library {}, deleting", libPath);
            Files.delete(link);
        }
        Files.createSymbolicLink(link, target);
        LOG.info("Symbolic link {} created for library {}", link, libPath);
    }
}
