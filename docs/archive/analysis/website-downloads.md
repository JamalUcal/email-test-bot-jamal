## Supplier Websites

# Neoparta
Angular app.  URL used to download the list of pricing files is @https://selfservice-backend.neoparta.com/api/Pricings/getGenuineSegmentsPriceList?PerPage=25&page=1 which gives response: {
    "Pagination": null,
    "Data": [
        {
            "Brand": "STOCK",
            "ValidFrom": null,
            "ValidTo": null,
            "BrandId": 1,
            "SegmentId": 1,
            "PartNo": 0,
            "IsSpecial": "false",
            "IsNewBrand": "false"
        },
       ... etc...
        {
            "Brand": "VOLVO",
            "ValidFrom": "2025-10-09",
            "ValidTo": null,
            "BrandId": 24,
            "SegmentId": 128,
            "PartNo": 0,
            "IsSpecial": "false",
            "IsNewBrand": "false"
        }
    ]
} we can then obtain the individual price lists using @https://selfservice-backend.neoparta.com/api/Pricings/exportByBrandId?brandId=42&segmentId=252&partNo=0  using the BrandId and SegmentId from the listing URL,. The Analysis tool seems a bit crap for this.


# Brechmann
	*Price list files URL:
	https://www.brechmann.parts/dashboard/price-list HTML Page with a list of available Brands in a select element:
	<optgroup label="Available brands">
                                                <option value="aa6d894a7c6b49ae9c7a316bd8d6290a">Hyundai</option>
                                                <option value="0c559628f3804bd393ed957cd31048a7">Lexus</option>
                                                <option value="fbe4e96fdb914889a2fa2a1f6b8f191e">Seat</option>
                                                <option value="feb872abc96441f281f810c3358e09bc">Skoda</option>
                                                <option value="409ef83545524e63bedf2a208182d8e1">Suzuki</option>
                                                <option value="f9b5333870bb4f8ba79f44569dcfae11">Toyota</option>
                                                <option value="ffff485ffdba4502839f02128f30030d">Volkswagen</option>
                                            </optgroup>
    Clicking Export triggers an email to be sent with the brand file attached
    URL:https://www.brechmann.parts/dashboard/price-list/export?productManufacturer=0c559628f3804bd393ed957cd31048a7&search_terms=
    Suucessful request rerouted to a new page: https://www.brechmann.parts/dashboard/price-list/export-success
    i.e. we can't directly download.
    
# APF:
Login: https://wiuse.net/customer/login
Price List: 
Static HTML page with download links in the format: \
										<div class="action-wrapper">                                      
                                            <button class="btn secondary ignore-button mr-16 margin-left-auto" data-brand="YUAS" style="width: min-content">
                                                Negeren
                                            </button>
                                        <a href="/pricelist/Download?brandCode=YUAS" target="_blank" class="btn primary download-btn" data-brand="YUAS" data-version="20.0.0.9" style="width: min-content">
                                            <span>Downloaden</span>
                                            <div class="icon-wrapper" style="display: none;">
                                                <div class="icon contain loader rotating"></div>
                                            </div>
                                        </a>                                       
                                    </div>
                                    
                                    Note site is in Dutch
# CONNEX:
    http://185.206.55.141/ucqIC0lqeurYC3d5sPv7VG/
    Behaves like an unsecured FTP site prompts for login on access
    Example HTML:
    <html><head><title>185.206.55.141 - /ucqIC0lqeurYC3d5sPv7VG/</title></head><body><h1>185.206.55.141 - /ucqIC0lqeurYC3d5sPv7VG/</h1><hr>
	<pre><a href="/">[To Parent Directory]</a><br><br>10/19/2025  9:55 PM        14403 <a href="/ucqIC0lqeurYC3d5sPv7VG/ams-OSRAM.csv">ams-OSRAM.csv</a><br>10/19/2025  9:55 PM      1046736 <a href="/ucqIC0lqeurYC3d5sPv7VG/AS-PL.csv">AS-PL.csv</a>....etc.
	Does not permit connection as an ftp site when I tried.
	

# Technoparts: 
Price List Location: https://storetp.technoparts.it/application/price-lists#month	
HTML with list of downloadable files with links in the format: <a class="btn-primary dnl-lm btndownlPrice" href="/price_list/BMW -  Export Price list October.xlsx" data-nome-file="BMW+-++EXPORT+PRICE+LIST+OCTOBER" target="_blank">Download</a>


# Materom:
Price List Location: http://109.96.101.208:8080/index.php/apps/files/files/445?dir=/Share%20Folder

HTML List.

Includes a Modified Column in the HTML which tell us how recently the file was uploaded i.e. the validity start

	<span data-timestamp="Mon Sep 15 2025 14:28:05 GMT+0100 (British Summer Time)">
	
Clicking a row accesses URL: http://109.96.101.208:8080/remote.php/dav/files/materom.user/Share%20Folder/WOLF_2025.xlsx

The file name is pulled from the <tr data-cy-files-list-row-name="WOLF_2025.xlsx" attribute.

Share folder contains special files:
Available Stock.xlsx
_Stock Clearance - we accept target prices.xlsx
Which have a different format and which we can ignore for now.


Full ROW HTML is:
<tr data-cy-files-list-row="" data-cy-files-list-row-fileid="12955" data-cy-files-list-row-name="WOLF_2025.xlsx" draggable="false" class="files-list__row" index="0">
	<!---->
	 <td class="files-list__row-checkbox">
	<span data-v-2c897dd5="" data-cy-files-list-row-checkbox="" class="checkbox-radio-switch checkbox-radio-switch-checkbox" style="--icon-size: 24px; --icon-height: 24px;">
	<input data-v-2c897dd5="" id="checkbox-radio-switch-fstcr" aria-label="Toggle selection for file &quot;WOLF_2025.xlsx&quot;" type="checkbox" class="checkbox-radio-switch__input" value="">
	<span data-v-3714b019="" data-v-2c897dd5="" class="checkbox-content checkbox-radio-switch__content checkbox-content-checkbox" id="checkbox-radio-switch-fstcr-label">
	<span data-v-3714b019="" aria-hidden="true" inert="inert" class="checkbox-content__icon checkbox-radio-switch__icon">
	<span data-v-3714b019="" aria-hidden="true" role="img" class="material-design-icon checkbox-blank-outline-icon">
	<svg fill="currentColor" width="24" height="24" viewBox="0 0 24 24" class="material-design-icon__svg">
	<path d="M19,3H5C3.89,3 3,3.89 3,5V19A2,2 0 0,0 5,21H19A2,2 0 0,0 21,19V5C21,3.89 20.1,3 19,3M19,5V19H5V5H19Z">
	<!---->
	</path>
	</svg>
	</span>
	</span>
	<!---->
	</span>
	</span>
	</td>
	 <td data-cy-files-list-row-name="" class="files-list__row-name">
	<span class="files-list__row-icon">
	<span class="files-list__row-icon-preview-container">
	<!---->
	 <img alt="" loading="lazy" src="http://109.96.101.208:8080/index.php/core/mimeicon?mime=application%2Fvnd.openxmlformats-officedocument.spreadsheetml.sheet" class="files-list__row-icon-preview files-list__row-icon-preview--loaded">
	</span>
	 <!---->
	 <!---->
	</span>
	 <button data-v-203ba9c0="" data-cy-files-list-row-name-link="" aria-label="Download" title="Download" tabindex="0" class="files-list__row-name-link">
	<span data-v-203ba9c0="" dir="auto" class="files-list__row-name-text">
	<span data-v-203ba9c0="" class="files-list__row-name-">
	WOLF_2025</span>
	 <span data-v-203ba9c0="" class="files-list__row-name-ext">
	.xlsx</span>
	</span>
	</button>
	</td>
	 <td data-v-d6a9a850="" data-cy-files-list-row-actions="" class="files-list__row-actions files-list__row-actions-2923347597">
	<span data-v-d6a9a850="" class="files-list__row-action--inline files-list__row-action-system-tags">
	<ul class="files-list__system-tags" aria-label="Assigned collaborative tags" data-systemtags-fileid="12955">
	</ul>
	</span>
	 <div data-v-9676f7ed="" data-v-d6a9a850="" class="action-items action-item--tertiary">
	<button data-v-ce3a06f2="" data-v-9676f7ed="" aria-label="Shared by Materom Admin" type="button" data-cy-files-list-row-action="sharing-status" title="Shared by Materom Admin" class="button-vue button-vue--size-normal button-vue--icon-and-text button-vue--vue-tertiary button-vue--legacy button-vue--tertiary action-item action-item--single files-list__row-action files-list__row-action-sharing-status files-list__row-action--inline">
	<span data-v-ce3a06f2="" class="button-vue__wrapper">
	<span data-v-ce3a06f2="" aria-hidden="true" class="button-vue__icon">
	<span data-v-a4f5b92e="" data-v-d6a9a850="" aria-hidden="true" role="img" class="icon-vue files-list__row-action-icon" data-v-ce3a06f2="" style="--adec40c8: 20px;">
	<span data-v-a4f5b92e="">
	<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32" viewBox="0 0 32 32" class="sharing-status__avatar">
	
		<image href="/index.php/avatar/materom.admin/32/dark?guestFallback=true" height="32" width="32">
	</image>
	
	</svg>
	</span>
	</span>
	</span>
	<span data-v-ce3a06f2="" class="button-vue__text">
	Shared</span>
	</span>
	</button>
	<div data-v-9676f7ed="" class="action-item">
	<div data-v-9676f7ed="" class="v-popper v-popper--theme-nc-popover-8">
	<button data-v-ce3a06f2="" data-v-9676f7ed="" aria-label="Actions" type="button" aria-haspopup="menu" aria-expanded="false" id="trigger-menu-tmvep" class="button-vue button-vue--size-normal button-vue--icon-only button-vue--vue-tertiary button-vue--legacy button-vue--tertiary action-item__menutoggle">
	<span data-v-ce3a06f2="" class="button-vue__wrapper">
	<span data-v-ce3a06f2="" aria-hidden="true" class="button-vue__icon">
	<span data-v-9676f7ed="" aria-hidden="true" role="img" class="material-design-icon dots-horizontal-icon" data-v-ce3a06f2="">
	<svg fill="currentColor" width="20" height="20" viewBox="0 0 24 24" class="material-design-icon__svg">
	<path d="M16,12A2,2 0 0,1 18,10A2,2 0 0,1 20,12A2,2 0 0,1 18,14A2,2 0 0,1 16,12M10,12A2,2 0 0,1 12,10A2,2 0 0,1 14,12A2,2 0 0,1 12,14A2,2 0 0,1 10,12M4,12A2,2 0 0,1 6,10A2,2 0 0,1 8,12A2,2 0 0,1 6,14A2,2 0 0,1 4,12Z">
	<!---->
	</path>
	</svg>
	</span>
	</span>
	</span>
	</button>
	</div>
	</div>
	</div>
	</td>
	 <td data-cy-files-list-row-size="" class="files-list__row-size" style="color: color-mix(in srgb, var(--color-main-text) 0%, var(--color-text-maxcontrast));">
	<span>
	64 KB</span>
	</td>
	 <td data-cy-files-list-row-mtime="" class="files-list__row-mtime">
		<span data-timestamp="Mon Sep 15 2025 14:28:05 GMT+0100 (British Summer Time)" title="15/09/2025, 14:28:05" class="nc-datetime">
	September 15</span>
	</td>
</tr>