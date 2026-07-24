using System.Net.Security;
using System.Reflection;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using AdamSalisbury.Meshworx.Transport.Tcp;

namespace AdamSalisbury.Meshworx.UnitTests.Transport.Tcp;

/// <summary>
/// Guards the TLS options-cloning helpers against silently dropping a setting.
/// </summary>
/// <remarks>
/// The helpers copy the caller's options property by property, which is the only way to snapshot these
/// mutable framework types. The hazard is that a property left out — whether by oversight now or because
/// a future .NET release adds one — discards the caller's intent without any error. For a security
/// setting that means quietly weakening the connection, so these tests drive every settable property by
/// reflection and fail loudly on anything the helpers do not carry across.
/// </remarks>
public sealed class TlsOptionsCloneTests
{
    /// <summary>
    /// Every settable property of SslClientAuthenticationOptions survives the clone.
    /// </summary>
    [Fact]
    public void CloneClientOptions_CopiesEverySettableProperty()
    {
        using X509Certificate2 certificate = TestCertificates.CreateSelfSigned();
        var source = new SslClientAuthenticationOptions();

        PropertyInfo[] properties = SettableProperties(typeof(SslClientAuthenticationOptions));
        var driven = new List<PropertyInfo>();

        foreach (PropertyInfo property in properties)
        {
            // TargetHost is deliberately not a straight copy — it is defaulted to the dialled host when
            // unset — so it is asserted separately below and by
            // TcpTransportTlsTests.ConnectAsync_TargetHostUnset_DefaultsToHostWithoutMutatingCallerOptions.
            if (property.Name == nameof(SslClientAuthenticationOptions.TargetHost))
            {
                continue;
            }

            if (!TrySetDistinctValue(property, source, certificate))
            {
                continue;
            }

            driven.Add(property);
        }

        SslClientAuthenticationOptions clone = TcpTransport.CloneClientOptions(source, "example.invalid");

        AssertPropertiesMatch(driven, source, clone);

        // The explicitly set value must win over the host fallback.
        source.TargetHost = "explicit.example";
        Assert.Equal(
            "explicit.example",
            TcpTransport.CloneClientOptions(source, "example.invalid").TargetHost);
    }

    /// <summary>
    /// Every settable property of SslServerAuthenticationOptions survives the clone.
    /// </summary>
    [Fact]
    public void CloneServerOptions_CopiesEverySettableProperty()
    {
        using X509Certificate2 certificate = TestCertificates.CreateSelfSigned();
        var source = new SslServerAuthenticationOptions();

        var driven = new List<PropertyInfo>();
        foreach (PropertyInfo property in SettableProperties(typeof(SslServerAuthenticationOptions)))
        {
            if (TrySetDistinctValue(property, source, certificate))
            {
                driven.Add(property);
            }
        }

        SslServerAuthenticationOptions clone = TcpTransportListener.CloneServerOptions(source);

        AssertPropertiesMatch(driven, source, clone);
    }

    private static PropertyInfo[] SettableProperties(Type type)
    {
        PropertyInfo[] properties = [.. type
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && p.CanWrite)];

        Assert.NotEmpty(properties);
        return properties;
    }

    private static void AssertPropertiesMatch(
        IReadOnlyList<PropertyInfo> properties,
        object source,
        object clone)
    {
        foreach (PropertyInfo property in properties)
        {
            Assert.Equal(
                property.GetValue(source),
                property.GetValue(clone));
        }
    }

    /// <summary>
    /// Sets the property to a value distinguishable from its default, so a clone that fails to copy it is
    /// detectable.
    /// </summary>
    /// <returns>
    /// <see langword="false"/> only when the value cannot be constructed on this platform. An unrecognised
    /// property type fails the test outright, which is the point: a property added by a future framework
    /// release must not slip past unnoticed.
    /// </returns>
    private static bool TrySetDistinctValue(PropertyInfo property, object target, X509Certificate2 certificate)
    {
        try
        {
            return SetDistinctValue(property, target, certificate);
        }
        catch (TargetInvocationException ex) when (ex.InnerException is PlatformNotSupportedException)
        {
            // Some settings — the RSA padding switches, cipher-suite selection — only exist on certain
            // platforms and throw when touched elsewhere. The helper guards them the same way; they
            // simply cannot be exercised on this host.
            return false;
        }
    }

    private static bool SetDistinctValue(PropertyInfo property, object target, X509Certificate2 certificate)
    {
        Type type = property.PropertyType;

        if (type == typeof(bool))
        {
            property.SetValue(target, !(bool)property.GetValue(target)!);
            return true;
        }

        if (type.IsEnum)
        {
            object current = property.GetValue(target)!;
            object? distinct = Enum.GetValues(type)
                .Cast<object>()
                .FirstOrDefault(v => !v.Equals(current));

            Assert.NotNull(distinct);
            property.SetValue(target, distinct);
            return true;
        }

        if (type == typeof(SslProtocols))
        {
            property.SetValue(target, SslProtocols.Tls12);
            return true;
        }

        if (type == typeof(string))
        {
            property.SetValue(target, "meshworx-probe");
            return true;
        }

        if (type == typeof(X509Certificate))
        {
            property.SetValue(target, certificate);
            return true;
        }

        if (type == typeof(X509CertificateCollection))
        {
            property.SetValue(target, new X509CertificateCollection { certificate });
            return true;
        }

        if (type == typeof(X509ChainPolicy))
        {
            property.SetValue(target, new X509ChainPolicy());
            return true;
        }

        if (type == typeof(List<SslApplicationProtocol>))
        {
            property.SetValue(target, new List<SslApplicationProtocol> { new("meshworx") });
            return true;
        }

        if (type == typeof(SslStreamCertificateContext))
        {
            property.SetValue(target, SslStreamCertificateContext.Create(certificate, []));
            return true;
        }

        if (type == typeof(CipherSuitesPolicy))
        {
            if (!OperatingSystem.IsLinux())
            {
                // Cipher-suite selection is a Linux-only knob. The helper copies the property regardless;
                // it just cannot be given a value to copy here.
                return false;
            }

            property.SetValue(target, new CipherSuitesPolicy([TlsCipherSuite.TLS_AES_128_GCM_SHA256]));
            return true;
        }

        if (typeof(Delegate).IsAssignableFrom(type))
        {
            property.SetValue(target, CreateDelegateFor(type));
            return true;
        }

        Assert.Fail(
            $"No probe value is defined for {property.DeclaringType?.Name}.{property.Name} of type "
                + $"{type.Name}. If the framework has added a property, add it to the clone helper in "
                + "TcpTransport/TcpTransportListener and to this test.");
        return false;
    }

    private static Delegate CreateDelegateFor(Type delegateType)
    {
        if (delegateType == typeof(RemoteCertificateValidationCallback))
        {
            return new RemoteCertificateValidationCallback((_, _, _, _) => false);
        }

        if (delegateType == typeof(LocalCertificateSelectionCallback))
        {
            return new LocalCertificateSelectionCallback((_, _, _, _, _) => null!);
        }

        if (delegateType == typeof(ServerCertificateSelectionCallback))
        {
            return new ServerCertificateSelectionCallback((_, _) => null!);
        }

        Assert.Fail($"No probe delegate is defined for {delegateType.Name}.");
        return null!;
    }
}
