package com.staterelay.starter.artifact;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * Artifact 目录的安全压缩、解压和校验工具。
 */
public final class SafeZipSupport {

    private SafeZipSupport() {
    }

    /**
     * 将目录压缩为 ZIP；源目录中的符号链接会被拒绝。
     *
     * @param sourceDirectory 源目录
     * @param zipFile ZIP 文件
     */
    public static void zipDirectory(Path sourceDirectory, Path zipFile) {
        Path source = requireDirectory(sourceDirectory);
        try {
            Files.createDirectories(zipFile.toAbsolutePath().normalize().getParent());
            try (OutputStream output = Files.newOutputStream(zipFile);
                 ZipOutputStream zip = new ZipOutputStream(output)) {
                Files.walkFileTree(source, new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult preVisitDirectory(Path directory, BasicFileAttributes attrs)
                            throws IOException {
                        rejectSymbolicLink(directory);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                            throws IOException {
                        rejectSymbolicLink(file);
                        String entryName = source.relativize(file).toString().replace('\\', '/');
                        zip.putNextEntry(new ZipEntry(entryName));
                        Files.copy(file, zip);
                        zip.closeEntry();
                        return FileVisitResult.CONTINUE;
                    }
                });
            }
        } catch (IOException e) {
            throw new IllegalStateException("压缩 Artifact 目录失败: " + source, e);
        }
    }

    /**
     * 安全解压 ZIP，拒绝绝对路径、父目录跳转和目标目录中的符号链接。
     *
     * @param zipFile ZIP 文件
     * @param destinationDirectory 目标目录
     * @return 解压后的目标目录
     */
    public static Path unzip(Path zipFile, Path destinationDirectory) {
        Path destination = destinationDirectory.toAbsolutePath().normalize();
        try {
            rejectExistingSymbolicLinks(destination);
            Files.createDirectories(destination);
            rejectSymbolicLink(destination);
            try (InputStream input = Files.newInputStream(zipFile);
                 ZipInputStream zip = new ZipInputStream(input)) {
                ZipEntry entry;
                while ((entry = zip.getNextEntry()) != null) {
                    Path target = resolveZipEntry(destination, entry.getName());
                    ensureNoSymbolicLink(destination, target.getParent());
                    if (Files.exists(target)) {
                        rejectSymbolicLink(target);
                    }
                    if (entry.isDirectory()) {
                        Files.createDirectories(target);
                    } else {
                        Files.createDirectories(target.getParent());
                        Files.copy(zip, target, StandardCopyOption.REPLACE_EXISTING);
                    }
                    zip.closeEntry();
                }
            }
            return destination;
        } catch (IOException e) {
            throw new IllegalStateException("解压 Artifact 目录失败: " + zipFile, e);
        }
    }

    /**
     * 计算文件 SHA-256，返回协议统一格式。
     *
     * @param file 文件路径
     * @return sha256:hex
     */
    public static String sha256(Path file) {
        try (InputStream input = Files.newInputStream(file)) {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[8192];
            int count;
            while ((count = input.read(buffer)) >= 0) {
                digest.update(buffer, 0, count);
            }
            return "sha256:" + HexFormat.of().formatHex(digest.digest());
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new IllegalStateException("计算 Artifact 校验和失败: " + file, e);
        }
    }

    private static Path requireDirectory(Path sourceDirectory) {
        Path source = sourceDirectory.toAbsolutePath().normalize();
        if (!Files.isDirectory(source)) {
            throw new IllegalArgumentException("Artifact 目录不存在: " + source);
        }
        return source;
    }

    private static Path resolveZipEntry(Path destination, String name) throws IOException {
        if (name == null || name.isBlank() || name.indexOf('\0') >= 0) {
            throw new IOException("ZIP 条目名称非法");
        }
        Path entryPath = Path.of(name);
        if (entryPath.isAbsolute() || name.startsWith("/") || name.startsWith("\\")) {
            throw new IOException("ZIP 条目不能使用绝对路径: " + name);
        }
        for (Path part : entryPath) {
            if ("..".equals(part.toString())) {
                throw new IOException("ZIP 条目不能包含父目录跳转: " + name);
            }
        }
        Path target = destination.resolve(entryPath).normalize();
        if (!target.startsWith(destination)) {
            throw new IOException("ZIP 条目越过目标目录: " + name);
        }
        return target;
    }

    private static void ensureNoSymbolicLink(Path root, Path path) throws IOException {
        if (path == null) {
            return;
        }
        Path current = root;
        Path relative = root.relativize(path.normalize());
        for (Path part : relative) {
            current = current.resolve(part);
            if (Files.exists(current)) {
                rejectSymbolicLink(current);
            }
        }
    }

    private static void rejectExistingSymbolicLinks(Path path) throws IOException {
        Path current = path.getRoot();
        if (current == null) {
            current = path.toAbsolutePath().getRoot();
        }
        for (Path part : path) {
            current = current == null ? part : current.resolve(part);
            if (Files.exists(current)) {
                rejectSymbolicLink(current);
            }
        }
    }

    private static void rejectSymbolicLink(Path path) throws IOException {
        if (Files.isSymbolicLink(path)) {
            throw new IOException("拒绝符号链接路径: " + path);
        }
    }
}
