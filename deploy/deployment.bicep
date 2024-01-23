param containerRegistryName string = '[YourACRNameHere]'
param aksName string = '[YourAKSNameHere]'
param location string = resourceGroup().location
param vmSize string = 'Standard_D3_v2'
param agentCount int = 7

resource containerRegistry 'Microsoft.ContainerRegistry/registries@2023-01-01-preview' = {
  name: containerRegistryName
  location: location
  sku: {
    name: 'Basic'
  }
  properties: {
    adminUserEnabled: true
  }
}

resource aksCluster 'Microsoft.ContainerService/managedClusters@2022-05-02-preview' = {
  name: aksName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    dnsPrefix: '${aksName}-dns'
    enableRBAC: true
    agentPoolProfiles: [
      {
        name: 'agentpool'
        count: agentCount
        vmSize: vmSize
        osType: 'Linux'
        mode: 'System'
      }
    ]
  }
}
 
param roleAcrPull string = '7f951dda-4ed3-4680-a7ca-43fe172d538d'
 
resource assignAcrPullToAks 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(resourceGroup().id, containerRegistryName, 'AssignAcrPullToAks')
  scope: containerRegistry
  properties: {
    description: 'Assign AcrPull role to AKS'
    principalId: aksCluster.properties.identityProfile.kubeletidentity.objectId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', roleAcrPull)
  }
}
