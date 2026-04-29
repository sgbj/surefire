import {type LucideIcon} from "lucide-react"
import {Link, useLocation} from "react-router"

import {
  SidebarGroup,
  SidebarGroupContent,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@/components/ui/sidebar"

export function NavMain({
                          items,
                        }: {
  items: {
    title: string
    url: string
    icon: LucideIcon
  }[]
}) {
  const location = useLocation()

  return (
    <SidebarGroup className="px-3 py-2.5">
      <SidebarGroupContent>
        <SidebarMenu>
          {items.map((item) => {
            const active = item.url === "/"
              ? location.pathname === "/"
              : location.pathname.startsWith(item.url)

            return (
              <SidebarMenuItem key={item.title}
                               className={active ? "before:absolute before:-left-1.5 before:top-1/2 before:-translate-y-1/2 before:h-5 before:w-[3px] before:bg-primary before:rounded-full" : ""}>
                <SidebarMenuButton asChild isActive={active} tooltip={item.title}>
                  <Link to={item.url}>
                    <item.icon className={active ? "opacity-80" : "opacity-60"}/>
                    <span>{item.title}</span>
                  </Link>
                </SidebarMenuButton>
              </SidebarMenuItem>
            )
          })}
        </SidebarMenu>
      </SidebarGroupContent>
    </SidebarGroup>
  )
}
