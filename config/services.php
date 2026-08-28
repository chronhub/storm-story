<?php

declare(strict_types=1);

namespace Symfony\Component\DependencyInjection\Loader\Configurator;

/*
 * Story package wiring.
 *
 * Registers the bus middleware, autowired and autoconfigured: AssignMessageMetadata autowires the
 * MetaIdentityGenerator, BindMessageContext the CurrentMessageContext. They are referenced by
 * id in each bus' middleware stack, in the messenger config, added with the buses. Stamps are value
 * objects, constructed with `new` on dispatch, not services.
 */
return static function (ContainerConfigurator $container): void {
    $services = $container->services()
        ->defaults()
        ->autowire()
        ->autoconfigure();

    $services->load('Storm\\Story\\', dirname(__DIR__).'/')
        ->exclude([
            dirname(__DIR__).'/Stamp/',
            dirname(__DIR__).'/Attribute/', // declarations, not services
            dirname(__DIR__).'/Query/', // Settled / QueryResults: value objects, private ctor + factories, not services
            dirname(__DIR__).'/Tests/',
            dirname(__DIR__).'/config/',
            dirname(__DIR__).'/Console/', // ConsumeBatchedCommand: registered explicitly, config-int args, in StormBundle
            dirname(__DIR__).'/Transport/InboxGuardedSendersLocator.php', // registered explicitly as the senders_locator decorator, in StormBundle
            // one instance per storm.neutral_transports entry, registered in StormBundle: the bus and
            // the allowlist ARE the wiring; a busless autowired instance would route decoded events
            // to the default bus, an event handler never found
            dirname(__DIR__).'/Transport/NeutralMessageSerializer.php',
            dirname(__DIR__).'/Consume/BatchHandlerFailed.php', // internal classification marker, not a service
            dirname(__DIR__).'/Consume/BatchDecision.php', // the batch's verdict, private ctor + factories, not a service
        ]);
};
