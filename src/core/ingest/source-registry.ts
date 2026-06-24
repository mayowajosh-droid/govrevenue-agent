export type SourceCategory =
  | "procurement_contracts"
  | "government_public"
  | "transport_infrastructure"
  | "health_education_social"
  | "environment_energy"
  | "news_media"
  | "crime_justice"
  | "other";

export type UpdateCadence = "realtime" | "daily" | "weekly" | "monthly";

export type DataSource = {
  id: string;
  name: string;
  category: SourceCategory;
  cadence: UpdateCadence;
  requiresKey: boolean;
  keyEnvVar?: string;
  live: boolean;
  description: string;
  baseUrl: string;
};

export const DATA_SOURCES: DataSource[] = [
  // PROCUREMENT & CONTRACTS (5)
  {
    id: "contracts_finder",
    name: "Contracts Finder",
    category: "procurement_contracts",
    cadence: "realtime",
    requiresKey: false,
    live: true,
    description: "UK public-sector contracts above £12k. Primary procurement data source.",
    baseUrl: "https://www.contractsfinder.service.gov.uk/Published/Notices/PublishedNoticeList/Search",
  },
  {
    id: "find_a_tender",
    name: "Find a Tender Service (FTS)",
    category: "procurement_contracts",
    cadence: "realtime",
    requiresKey: false,
    live: true,
    description: "Above-threshold OJEU replacement notices. Cabinet Office.",
    baseUrl: "https://www.find-tender.service.gov.uk/api/1.0/ocdsReleasePackages",
  },
  {
    id: "sell2wales",
    name: "Sell2Wales",
    category: "procurement_contracts",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "Welsh Government procurement portal. RSS feed.",
    baseUrl: "https://www.sell2wales.gov.wales/rss/rss_switch.aspx",
  },
  {
    id: "nhsbsa_pipeline",
    name: "NHSBSA Pipeline (CKAN)",
    category: "procurement_contracts",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description: "NHS Business Services Authority open data and procurement pipeline.",
    baseUrl: "https://opendata.nhsbsa.net/api/3/action",
  },
  {
    id: "gazette",
    name: "The Gazette",
    category: "procurement_contracts",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "Official UK government gazette — insolvency, company, and procurement notices.",
    baseUrl: "https://www.thegazette.co.uk",
  },

  // GOVERNMENT & PUBLIC DATA (10)
  {
    id: "ons",
    name: "ONS API",
    category: "government_public",
    cadence: "monthly",
    requiresKey: false,
    live: true,
    description: "Office for National Statistics — construction output, business demography, GDP.",
    baseUrl: "https://api.ons.gov.uk",
  },
  {
    id: "nomis",
    name: "Nomis API",
    category: "government_public",
    cadence: "monthly",
    requiresKey: false,
    live: true,
    description: "ONS labour market data — employment, business counts, BRES by sector and area.",
    baseUrl: "https://www.nomisweb.co.uk/api/v01",
  },
  {
    id: "companies_house",
    name: "Companies House",
    category: "government_public",
    cadence: "daily",
    requiresKey: true,
    keyEnvVar: "COMPANIES_HOUSE_API_KEY",
    live: true,
    description: "Company registration, officers, PSCs, filing history.",
    baseUrl: "https://api.company-information.service.gov.uk",
  },
  {
    id: "charity_commission",
    name: "Charity Commission",
    category: "government_public",
    cadence: "weekly",
    requiresKey: true,
    keyEnvVar: "CHARITY_COMMISSION_API_KEY",
    live: true,
    description: "England & Wales charity register — income, status, activities.",
    baseUrl: "https://api.charitycommission.gov.uk/register/api",
  },
  {
    id: "uk_parliament",
    name: "UK Parliament API",
    category: "government_public",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "Written questions, committee reports, Hansard debates.",
    baseUrl: "https://questions-statements-api.parliament.uk/api",
  },
  {
    id: "govuk_content",
    name: "GOV.UK Content API",
    category: "government_public",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "Policy publications, consultations, spending reviews, strategies.",
    baseUrl: "https://www.gov.uk/api/search.json",
  },
  {
    id: "planning_data",
    name: "Planning Data API (DLUHC)",
    category: "government_public",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "Brownfield land, conservation areas, Article 4 directions.",
    baseUrl: "https://www.planning.data.gov.uk/api/v1",
  },
  {
    id: "land_registry",
    name: "HM Land Registry (HMLR)",
    category: "government_public",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description: "Property transaction prices by district. Price Paid Data API.",
    baseUrl: "https://landregistry.data.gov.uk/data/ppi",
  },
  {
    id: "local_authority_spending",
    name: "Local Authority Spending (data.gov.uk)",
    category: "government_public",
    cadence: "monthly",
    requiresKey: false,
    live: true,
    description: "Local authority spending over £500 datasets indexed on data.gov.uk.",
    baseUrl: "https://data.gov.uk/api/action",
  },
  {
    id: "data_gov_uk",
    name: "data.gov.uk CKAN API",
    category: "government_public",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "UK open government data — CKAN-based search across all public datasets.",
    baseUrl: "https://data.gov.uk/api/action",
  },

  // TRANSPORT & INFRASTRUCTURE (6)
  {
    id: "tfl",
    name: "TfL Unified API",
    category: "transport_infrastructure",
    cadence: "realtime",
    requiresKey: false,
    live: true,
    description: "Transport for London — line statuses, stop points, disruptions.",
    baseUrl: "https://api.tfl.gov.uk",
  },
  {
    id: "rail_data",
    name: "Rail Data Marketplace (ORR)",
    category: "transport_infrastructure",
    cadence: "monthly",
    requiresKey: false,
    live: true,
    description: "Office of Rail and Road — station usage, performance statistics.",
    baseUrl: "https://dataportal.orr.gov.uk",
  },
  {
    id: "bus_open_data",
    name: "Bus Open Data Service",
    category: "transport_infrastructure",
    cadence: "daily",
    requiresKey: true,
    keyEnvVar: "BODS_API_KEY",
    live: true,
    description: "DfT Bus Open Data Service — operators, datasets, timetables.",
    baseUrl: "https://data.bus-data.dft.gov.uk/api/v1",
  },
  {
    id: "networkrail",
    name: "NetworkRail Open Data",
    category: "transport_infrastructure",
    cadence: "daily",
    requiresKey: true,
    keyEnvVar: "NETWORK_RAIL_TOKEN",
    live: true,
    description: "NetworkRail data feeds — train running data, schedule, assets.",
    baseUrl: "https://datafeeds.networkrail.co.uk",
  },
  {
    id: "os_names",
    name: "OS Names API",
    category: "transport_infrastructure",
    cadence: "daily",
    requiresKey: true,
    keyEnvVar: "OS_API_KEY",
    live: true,
    description: "Ordnance Survey place name search — settlements, roads, geography.",
    baseUrl: "https://api.os.uk/search/names/v1",
  },
  {
    id: "postcodes_io",
    name: "Postcodes.io",
    category: "transport_infrastructure",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "Free UK postcode lookup — district, constituency, lat/lon, NHS area.",
    baseUrl: "https://api.postcodes.io",
  },

  // HEALTH, EDUCATION & SOCIAL (6)
  {
    id: "nhs_content",
    name: "NHS ODS (Organisation Data Service)",
    category: "health_education_social",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "NHS organisational directory — trusts, CCGs, GP practices.",
    baseUrl: "https://directory.spineservices.nhs.uk/ORD/2-0-0",
  },
  {
    id: "food_hygiene",
    name: "Food Hygiene Ratings API (FSA)",
    category: "health_education_social",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description: "Food Standards Agency ratings for catering/hospitality sector intel.",
    baseUrl: "https://ratings.food.gov.uk",
  },
  {
    id: "explore_education",
    name: "Explore Education (DfE)",
    category: "health_education_social",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description: "DfE school information — establishments, types, local authority.",
    baseUrl: "https://get-information-schools.service.gov.uk",
  },
  {
    id: "unistats",
    name: "Unistats / Discover Uni",
    category: "health_education_social",
    cadence: "monthly",
    requiresKey: false,
    live: false,
    description: "Higher education course data. Public API deprecated; data via HESA.",
    baseUrl: "https://discoveruni.gov.uk",
  },
  {
    id: "three_sixty_giving",
    name: "360Giving GrantNav",
    category: "health_education_social",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description: "UK grant-making data — funding organisations, recipients, amounts.",
    baseUrl: "https://grantnav.threesixtygiving.org/api",
  },
  {
    id: "find_that_charity",
    name: "Find That Charity (FTC)",
    category: "health_education_social",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description: "Charity search aggregator — CH numbers, income, registration data.",
    baseUrl: "https://findthatcharity.uk",
  },

  // ENVIRONMENT & ENERGY (5)
  {
    id: "met_office",
    name: "Met Office DataHub",
    category: "environment_energy",
    cadence: "daily",
    requiresKey: true,
    keyEnvVar: "MET_OFFICE_API_KEY",
    live: true,
    description: "Weather forecasts and warnings — relevant for construction/energy desks.",
    baseUrl: "https://data.hub.api.metoffice.gov.uk",
  },
  {
    id: "environment_agency",
    name: "Environment Agency APIs",
    category: "environment_energy",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "Flood warnings, monitoring stations, water quality data.",
    baseUrl: "https://environment.data.gov.uk/flood-monitoring",
  },
  {
    id: "defra",
    name: "Defra Data Services",
    category: "environment_energy",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description: "Environmental permits, waste data, air quality, agricultural statistics.",
    baseUrl: "https://data.gov.uk",
  },
  {
    id: "octopus_energy",
    name: "Octopus Energy API",
    category: "environment_energy",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "Energy product and tariff data — green energy, variable rates.",
    baseUrl: "https://api.octopus.energy/v1",
  },
  {
    id: "natural_resources_wales",
    name: "Natural Resources Wales APIs",
    category: "environment_energy",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description: "NRW open data — water quality, forestry, biodiversity.",
    baseUrl: "https://data.gov.uk",
  },

  // NEWS & MEDIA (1)
  {
    id: "bbc_news",
    name: "BBC News Labs APIs",
    category: "news_media",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "BBC News RSS feeds — business, politics, UK news for signal context.",
    baseUrl: "https://feeds.bbci.co.uk/news",
  },

  // CRIME & JUSTICE (2)
  {
    id: "uk_police",
    name: "UK Police API",
    category: "crime_justice",
    cadence: "monthly",
    requiresKey: false,
    live: true,
    description: "Crime data by location and force — contextual intel for security/facilities desks.",
    baseUrl: "https://data.police.uk/api",
  },
  {
    id: "courts_tribunals",
    name: "Courts & Tribunals API",
    category: "crime_justice",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description: "Court finder — areas of law, court types, locations.",
    baseUrl: "https://www.find-court-tribunal.service.gov.uk",
  },

  // OTHER (2)
  {
    id: "hansard",
    name: "Hansard API",
    category: "other",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "Parliamentary debates — procurement, construction, health, energy mentions.",
    baseUrl: "https://hansard-api.parliament.uk",
  },
  {
    id: "energy_tech_list",
    name: "ETL API (Energy Tech List)",
    category: "other",
    cadence: "monthly",
    requiresKey: false,
    live: true,
    description: "DESNZ approved energy-efficient product list. Indexed via data.gov.uk.",
    baseUrl: "https://etl.beis.gov.uk",
  },

  // ── MISSING 6 — closes gap to 43 ─────────────────────────────────────────

  // ONS core — already used in early-signals; register here for ingest tracking
  {
    id: "ons",
    name: "ONS Construction & Business Data",
    category: "government_public",
    cadence: "monthly",
    requiresKey: false,
    live: true,
    description: "ONS construction output + business demography indicators. Used for early-signal generation.",
    baseUrl: "https://api.beta.ons.gov.uk/v1",
  },

  // GOV.UK content — already used in early-signals; register here for ingest tracking
  {
    id: "govuk_content",
    name: "GOV.UK Content API",
    category: "government_public",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "GOV.UK content and policy announcements relevant to procurement sectors.",
    baseUrl: "https://www.gov.uk/api/content",
  },

  // Public Contracts Scotland
  {
    id: "public_contracts_scotland",
    name: "Public Contracts Scotland",
    category: "procurement_contracts",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "Scottish Government procurement portal. RSS notice feed.",
    baseUrl: "https://www.publiccontractsscotland.gov.uk",
  },

  // eSourcing NI — Northern Ireland
  {
    id: "esourcing_ni",
    name: "eSourcing NI",
    category: "procurement_contracts",
    cadence: "daily",
    requiresKey: false,
    live: true,
    description: "Northern Ireland public procurement notices. Indexed via data.gov.uk CKAN.",
    baseUrl: "https://data.gov.uk",
  },

  // CQC
  {
    id: "cqc",
    name: "Care Quality Commission",
    category: "health_education_social",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description: "CQC registered providers and inspection outcomes. Free public API.",
    baseUrl: "https://api.cqc.org.uk/public/v1",
  },

  // Ofsted
  {
    id: "ofsted",
    name: "Ofsted Inspection Data",
    category: "health_education_social",
    cadence: "weekly",
    requiresKey: false,
    live: true,
    description: "Ofsted school and provider inspection datasets. Indexed via data.gov.uk.",
    baseUrl: "https://data.gov.uk",
  },
];

export function getSource(id: string): DataSource | undefined {
  return DATA_SOURCES.find(s => s.id === id);
}

export function getSourcesByCategory(category: SourceCategory): DataSource[] {
  return DATA_SOURCES.filter(s => s.category === category);
}

export function getLiveSources(): DataSource[] {
  return DATA_SOURCES.filter(s => s.live);
}
